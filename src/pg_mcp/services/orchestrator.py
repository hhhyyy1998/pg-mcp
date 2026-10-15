"""Query orchestrator for coordinating the complete query flow.

This module provides the QueryOrchestrator class that coordinates all components
of the query processing pipeline: SQL generation, validation, execution, and result
validation. It implements retry logic with exponential backoff, rate limiting,
metrics collection, circuit breaker pattern, distributed request tracing, and
per-database routing.
"""

import asyncio
import logging
import time
from typing import Any

from asyncpg import Pool

from pg_mcp.cache.schema_cache import SchemaCache
from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.models.errors import (
    DatabaseError,
    ErrorCode,
    LLMError,
    PgMcpError,
    SchemaLoadError,
    SecurityViolationError,
    SQLParseError,
)
from pg_mcp.models.query import (
    ErrorDetail,
    QueryRequest,
    QueryResponse,
    QueryResult,
    ReturnType,
    ValidationResult,
)
from pg_mcp.observability.metrics import MetricsCollector
from pg_mcp.observability.tracing import request_context
from pg_mcp.resilience.circuit_breaker import CircuitBreaker
from pg_mcp.resilience.rate_limiter import MultiRateLimiter
from pg_mcp.services.result_validator import ResultValidator
from pg_mcp.services.sql_executor import SQLExecutor
from pg_mcp.services.sql_generator import SQLGenerator
from pg_mcp.services.sql_validator import SQLValidator

logger = logging.getLogger(__name__)


class QueryOrchestrator:
    """Orchestrates the complete query processing pipeline.

    This class coordinates SQL generation, validation, execution, and result
    validation. It implements retry logic with error feedback, circuit breaker
    pattern for fault tolerance, and comprehensive error handling.

    Example:
        >>> orchestrator = QueryOrchestrator(
        ...     sql_generator=generator,
        ...     sql_validator=validator,
        ...     sql_executor=executor,
        ...     result_validator=result_validator,
        ...     schema_cache=cache,
        ...     pools={"mydb": pool},
        ...     resilience_config=resilience_config,
        ...     validation_config=validation_config,
        ... )
        >>> response = await orchestrator.execute_query(QueryRequest(
        ...     question="How many users?",
        ...     database="mydb"
        ... ))
    """

    def __init__(
        self,
        sql_generator: SQLGenerator,
        sql_validator: SQLValidator,
        sql_executors: dict[str, SQLExecutor],
        result_validator: ResultValidator,
        schema_cache: SchemaCache,
        pools: dict[str, Pool],
        resilience_config: ResilienceConfig,
        validation_config: ValidationConfig,
        rate_limiter: MultiRateLimiter | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        """Initialize query orchestrator.

        Args:
            sql_generator: SQL generation service.
            sql_validator: SQL validation service.
            sql_executors: Dictionary mapping database names to SQLExecutor instances
                (FIX: previously single executor, now per-database routing).
            result_validator: Result validation service.
            schema_cache: Schema cache instance.
            pools: Dictionary mapping database names to connection pools.
            resilience_config: Resilience configuration for retries and circuit breaker.
            validation_config: Validation configuration including thresholds.
            rate_limiter: MultiRateLimiter for concurrency control (FIX: previously unused).
            metrics: MetricsCollector for Prometheus metrics (FIX: previously unused).
        """
        self.sql_generator = sql_generator
        self.sql_validator = sql_validator
        self.sql_executors = sql_executors  # FIX: dict[str, SQLExecutor] instead of single executor
        self.result_validator = result_validator
        self.schema_cache = schema_cache
        self.pools = pools
        self.resilience_config = resilience_config
        self.validation_config = validation_config
        self.rate_limiter = rate_limiter      # FIX: wired in
        self.metrics = metrics                # FIX: wired in

        # Circuit breaker for LLM calls
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=resilience_config.circuit_breaker_threshold,
            recovery_timeout=resilience_config.circuit_breaker_timeout,
        )

    async def execute_query(self, request: QueryRequest) -> QueryResponse:
        """Execute complete query flow from question to results.

        This method orchestrates the entire pipeline with:
        1. Distributed request tracing (FIX: was bare manual ID)
        2. Per-database routing to correct executor (FIX: was single executor)
        3. Rate limiting for DB queries (FIX: was dead code)
        4. Metrics collection on every hop (FIX: was dead code)
        5. SQL generation with exponential-backoff retry
        6. Execution and result validation

        Args:
            request: Query request containing question and parameters.

        Returns:
            QueryResponse: Complete response with SQL, results, or error information.
        """
        # FIX 3.4: Use request_context for proper tracing propagation
        # instead of bare manual ID generation
        async with request_context() as request_id:
            logger.info(
                "Starting query execution",
                extra={"request_id": request_id, "question": request.question[:100]},
            )

            start_total = time.monotonic()

            # FIX 2.3: Metrics — count incoming request
            if self.metrics:
                self.metrics.increment_query_request(status="received", database=request.database or "auto")

            try:
                # Step 1: Resolve database name and find its executor
                database_name = self._resolve_database(request.database)
                logger.debug(
                    "Resolved database",
                    extra={"request_id": request_id, "database": database_name},
                )

                # FIX 1.1: Route to correct per-database executor
                executor = self.sql_executors.get(database_name)
                if executor is None:
                    raise DatabaseError(
                        message=f"No SQL executor available for database '{database_name}'",
                        details={
                            "database": database_name,
                            "available_executors": list(self.sql_executors.keys()),
                        },
                    )

                # Step 2: Get schema from cache
                schema = self.schema_cache.get(database_name)
                if schema is None:
                    # Schema not in cache, load it
                    pool = self.pools.get(database_name)
                    if pool is None:
                        raise DatabaseError(
                            message=f"No connection pool available for database '{database_name}'",
                            details={"database": database_name},
                        )
                    try:
                        schema = await self.schema_cache.load(database_name, pool)
                    except Exception as e:
                        raise SchemaLoadError(
                            message=f"Failed to load schema for database '{database_name}': {e!s}",
                            details={"database": database_name, "error": str(e)},
                        ) from e

                logger.debug(
                    "Schema loaded",
                    extra={
                        "request_id": request_id,
                        "database": database_name,
                        "tables": len(schema.tables),
                    },
                )

                # Step 3: Generate and validate SQL with retry logic
                generated_sql, validation_result, tokens_used = await self._generate_sql_with_retry(
                    question=request.question,
                    schema=schema,
                    request_id=request_id,
                )

                # Step 4: If return_type is SQL, return early
                if request.return_type == ReturnType.SQL:
                    logger.info(
                        "Returning SQL only",
                        extra={"request_id": request_id, "sql_length": len(generated_sql)},
                    )
                    total_duration = time.monotonic() - start_total
                    if self.metrics:
                        self.metrics.query_duration.observe(total_duration)
                        self.metrics.increment_query_request(status="success", database=database_name)
                    return QueryResponse(
                        success=True,
                        generated_sql=generated_sql,
                        validation=validation_result,
                        data=None,
                        error=None,
                        confidence=100,
                        tokens_used=tokens_used,
                    )

                # Step 5: Execute SQL — FIX 2.1: wrap with rate limiter
                logger.debug("Executing SQL", extra={"request_id": request_id})
                start_time = time.monotonic()

                try:
                    if self.rate_limiter is not None:
                        async with self.rate_limiter.for_queries(timeout=30.0):
                            results, total_count = await executor.execute(generated_sql)
                    else:
                        results, total_count = await executor.execute(generated_sql)
                except TimeoutError:
                    from pg_mcp.models.errors import RateLimitExceededError
                    # Rate limiter timeout
                    raise RateLimitExceededError(
                        message="Rate limiter timeout — too many concurrent queries",
                        details={"database": database_name},
                    )

                execution_time_ms = (time.monotonic() - start_time) * 1000
                logger.info(
                    "SQL executed successfully",
                    extra={
                        "request_id": request_id,
                        "row_count": total_count,
                        "execution_time_ms": execution_time_ms,
                    },
                )

                # FIX 2.3: Metrics — DB query duration
                if self.metrics:
                    self.metrics.observe_db_query_duration(execution_time_ms / 1000.0)

                # Step 6: Validate results (non-blocking, failures don't fail the request)
                result_confidence = await self._validate_results_safely(
                    question=request.question,
                    sql=generated_sql,
                    results=results,
                    row_count=total_count,
                    request_id=request_id,
                )

                # Step 7: Build successful response
                query_result = QueryResult(
                    columns=list(results[0].keys()) if results else [],
                    rows=results,
                    row_count=len(results),  # Limited row count (after max_rows applied)
                    execution_time_ms=execution_time_ms,
                )

                total_duration = time.monotonic() - start_total
                if self.metrics:
                    self.metrics.query_duration.observe(total_duration)
                    self.metrics.increment_query_request(status="success", database=database_name)

                return QueryResponse(
                    success=True,
                    generated_sql=generated_sql,
                    validation=validation_result,
                    data=query_result,
                    error=None,
                    confidence=result_confidence,
                    tokens_used=tokens_used,
                )

            except PgMcpError as e:
                # Handle known application errors
                logger.warning(
                    "Query execution failed with known error",
                    extra={
                        "request_id": request_id,
                        "error_code": e.code,
                        "error_message": str(e),
                    },
                )
                total_duration = time.monotonic() - start_total
                if self.metrics:
                    self.metrics.query_duration.observe(total_duration)
                    db = self._safe_db_name(request.database)
                    self.metrics.increment_query_request(status="error", database=db)
                    self.metrics.increment_sql_rejected(reason=str(e.code))
                return QueryResponse(
                    success=False,
                    generated_sql=None,
                    validation=None,
                    data=None,
                    error=ErrorDetail(
                        code=str(e.code),      # StrEnum → str
                        message=e.message,
                        details=e.details,
                    ),
                    confidence=0,
                    tokens_used=None,
                )
            except Exception as e:
                # Handle unexpected errors
                logger.exception(
                    "Query execution failed with unexpected error",
                    extra={"request_id": request_id},
                )
                total_duration = time.monotonic() - start_total
                if self.metrics:
                    self.metrics.query_duration.observe(total_duration)
                    db = self._safe_db_name(request.database)
                    self.metrics.increment_query_request(status="internal_error", database=db)
                return QueryResponse(
                    success=False,
                    generated_sql=None,
                    validation=None,
                    data=None,
                    error=ErrorDetail(
                        code=ErrorCode.INTERNAL_ERROR.value,
                        message=f"Internal server error: {e!s}",
                        details={"error_type": type(e).__name__},
                    ),
                    confidence=0,
                    tokens_used=None,
                )

    def _resolve_database(self, database: str | None) -> str:
        """Resolve database name from request or auto-select.

        If database is specified, validate it exists.
        If not specified and only one database available, auto-select it.

        Args:
            database: Database name from request (optional).

        Returns:
            str: Resolved database name.

        Raises:
            DatabaseError: If database is invalid or cannot be auto-selected.

        Example:
            >>> name = orchestrator._resolve_database("mydb")  # Validates "mydb" exists
            >>> name = orchestrator._resolve_database(None)  # Auto-selects if only one DB
        """
        if database is not None:
            # Validate specified database exists
            if database not in self.pools:
                raise DatabaseError(
                    message=f"Database '{database}' not found",
                    details={
                        "requested_database": database,
                        "available_databases": list(self.pools.keys()),
                    },
                )
            return database

        # Auto-select if only one database available
        available_dbs = list(self.pools.keys())
        if len(available_dbs) == 0:
            raise DatabaseError(
                message="No databases configured",
                details={},
            )
        if len(available_dbs) == 1:
            return available_dbs[0]

        # Multiple databases, must specify
        raise DatabaseError(
            message="Multiple databases available, please specify which to query",
            details={"available_databases": available_dbs},
        )

    async def _generate_sql_with_retry(
        self,
        question: str,
        schema: Any,
        request_id: str,
    ) -> tuple[str, ValidationResult, int | None]:
        """Generate and validate SQL with retry logic on validation failures.

        This method implements a retry loop that:
        1. Checks circuit breaker state
        2. Generates SQL using LLM
        3. Validates the generated SQL
        4. On validation failure, retries with error feedback
        5. Records success/failure to circuit breaker

        Args:
            question: User's natural language question.
            schema: Database schema for context.
            request_id: Request ID for tracking.

        Returns:
            tuple: (generated_sql, validation_result, tokens_used)

        Raises:
            LLMError: If circuit breaker is open or generation fails.
            SecurityViolationError: If SQL fails validation after all retries.
            SQLParseError: If SQL cannot be parsed.

        Example:
            >>> sql, validation, tokens = await orchestrator._generate_sql_with_retry(
            ...     question="Count users",
            ...     schema=db_schema,
            ...     request_id="123",
            ... )
        """
        # Check circuit breaker
        if not self.circuit_breaker.allow_request():
            raise LLMError(
                message="SQL generation service is temporarily unavailable (circuit breaker open)",
                details={
                    "circuit_state": self.circuit_breaker.state,
                    "failure_count": self.circuit_breaker.failure_count,
                },
            )

        previous_sql: str | None = None
        error_feedback: str | None = None
        max_retries = self.resilience_config.max_retries
        # FIX 2.2: Use exponential backoff from ResilienceConfig
        base_delay = self.resilience_config.retry_delay
        backoff_factor = self.resilience_config.backoff_factor
        tokens_used: int | None = None

        for attempt in range(max_retries + 1):
            try:
                # FIX 2.2: Exponential backoff — wait before retry (not on first attempt)
                if attempt > 0:
                    delay = base_delay * (backoff_factor ** attempt)
                    logger.info(
                        "Exponential backoff before retry",
                        extra={
                            "request_id": request_id,
                            "attempt": attempt + 1,
                            "delay_seconds": round(delay, 3),
                        },
                    )
                    await asyncio.sleep(delay)

                logger.debug(
                    "Generating SQL",
                    extra={
                        "request_id": request_id,
                        "attempt": attempt + 1,
                        "max_retries": max_retries + 1,
                    },
                )

                # Generate SQL
                generated_sql = await self.sql_generator.generate(
                    question=question,
                    schema=schema,
                    previous_attempt=previous_sql,
                    error_feedback=error_feedback,
                )

                # Note: tokens_used would come from OpenAI response metadata if available
                # For now, we don't extract it, but it can be added later

                # FIX 2.3: Metrics — LLM calls
                if self.metrics:
                    self.metrics.increment_llm_call(operation="generate_sql")

                logger.debug(
                    "SQL generated",
                    extra={
                        "request_id": request_id,
                        "sql_length": len(generated_sql),
                    },
                )

                # Validate SQL
                try:
                    self.sql_validator.validate_or_raise(generated_sql)
                except (SecurityViolationError, SQLParseError) as validation_error:
                    if attempt < max_retries:
                        # Record as failure and retry with feedback
                        logger.warning(
                            "SQL validation failed, retrying with feedback",
                            extra={
                                "request_id": request_id,
                                "attempt": attempt + 1,
                                "error": str(validation_error),
                            },
                        )
                        previous_sql = generated_sql
                        error_feedback = str(validation_error)
                        continue
                    else:
                        # Out of retries, record failure and raise
                        self.circuit_breaker.record_failure()
                        logger.error(
                            "SQL validation failed after all retries",
                            extra={
                                "request_id": request_id,
                                "attempts": attempt + 1,
                                "error": str(validation_error),
                            },
                        )
                        raise

                # Validation successful
                self.circuit_breaker.record_success()
                logger.info(
                    "SQL generated and validated successfully",
                    extra={
                        "request_id": request_id,
                        "attempts": attempt + 1,
                    },
                )

                # Build validation result
                validation_result = ValidationResult(
                    is_valid=True,
                    is_select=True,
                    allows_data_modification=False,
                    uses_blocked_functions=[],
                    error_message=None,
                )

                return generated_sql, validation_result, tokens_used

            except (LLMError, SecurityViolationError, SQLParseError):
                # Re-raise known errors
                raise
            except Exception as e:
                # Unexpected error during generation
                self.circuit_breaker.record_failure()
                logger.exception(
                    "Unexpected error during SQL generation",
                    extra={"request_id": request_id},
                )
                raise LLMError(
                    message=f"SQL generation failed unexpectedly: {e!s}",
                    details={"error_type": type(e).__name__},
                ) from e

        # Should not reach here, but just in case
        self.circuit_breaker.record_failure()
        raise LLMError(
            message="SQL generation failed after all retry attempts",
            details={"max_retries": max_retries},
        )

    async def _validate_results_safely(
        self,
        question: str,
        sql: str,
        results: list[dict[str, Any]],
        row_count: int,
        request_id: str,
    ) -> int:
        """Validate query results with error handling (non-blocking).

        This method attempts to validate results using LLM, but failures
        don't cause the overall query to fail. Returns a confidence score.

        Args:
            question: User's original question.
            sql: Generated SQL query.
            results: Query results.
            row_count: Total row count.
            request_id: Request ID for tracking.

        Returns:
            int: Confidence score (0-100). Returns 100 if validation disabled/fails.

        Example:
            >>> confidence = await orchestrator._validate_results_safely(
            ...     question="Count users",
            ...     sql="SELECT COUNT(*) FROM users",
            ...     results=[{"count": 42}],
            ...     row_count=1,
            ...     request_id="123",
            ... )
        """
        if not self.validation_config.enabled:
            return 100

        try:
            logger.debug(
                "Validating results",
                extra={"request_id": request_id},
            )

            validation_result = await self.result_validator.validate(
                question=question,
                sql=sql,
                results=results,
                row_count=row_count,
            )

            logger.info(
                "Result validation completed",
                extra={
                    "request_id": request_id,
                    "confidence": validation_result.confidence,
                    "is_acceptable": validation_result.is_acceptable,
                },
            )

            return validation_result.confidence

        except Exception as e:
            # Log but don't fail the query
            logger.warning(
                "Result validation failed, continuing with default confidence",
                extra={
                    "request_id": request_id,
                    "error": str(e),
                },
            )
            return 100  # Default to high confidence if validation fails

    @staticmethod
    def _safe_db_name(database: str | None) -> str:
        """Return a safe database name for metrics label (fallback to 'auto')."""
        return database or "auto"
