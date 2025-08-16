"""
Logging configuration for Singapore Company Database ETL Pipeline
Provides structured logging with multiple output formats and levels
"""

import os
import sys
import logging
import logging.handlers
from typing import Optional, Dict, Any
from datetime import datetime
import json
import structlog
from loguru import logger as loguru_logger

from src.config import settings


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, default=str)


class ETLLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter for ETL pipeline with context"""
    
    def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None):
        super().__init__(logger, extra or {})
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process log message with additional context"""
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        
        # Add pipeline context
        kwargs['extra'].update(self.extra)
        
        # Add performance metrics if available
        if hasattr(self, '_start_time'):
            kwargs['extra']['duration_ms'] = int((datetime.now().timestamp() - self._start_time) * 1000)
        
        return msg, kwargs
    
    def start_timer(self):
        """Start performance timer"""
        self._start_time = datetime.now().timestamp()
    
    def log_performance(self, operation: str, **metrics):
        """Log performance metrics"""
        self.info(f"Performance: {operation}", extra={'metrics': metrics})
    
    def log_data_quality(self, check_type: str, result: str, details: Dict[str, Any]):
        """Log data quality check results"""
        self.info(f"Data Quality Check: {check_type}", extra={
            'check_type': check_type,
            'result': result,
            'details': details
        })
    
    def log_extraction(self, source: str, records_extracted: int, errors: int = 0):
        """Log data extraction results"""
        self.info(f"Data Extraction: {source}", extra={
            'source': source,
            'records_extracted': records_extracted,
            'errors': errors,
            'success_rate': (records_extracted / (records_extracted + errors)) * 100 if (records_extracted + errors) > 0 else 0
        })
    
    def log_llm_processing(self, model: str, prompt_type: str, tokens_used: int, processing_time_ms: int):
        """Log LLM processing metrics"""
        self.info(f"LLM Processing: {prompt_type}", extra={
            'model': model,
            'prompt_type': prompt_type,
            'tokens_used': tokens_used,
            'processing_time_ms': processing_time_ms,
            'tokens_per_second': tokens_used / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        })


def setup_logging() -> None:
    """Setup logging configuration for the application"""
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(settings.logging.file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.logging.level))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, settings.logging.level))
    
    if settings.logging.enable_structured_logging:
        console_handler.setFormatter(StructuredFormatter())
    else:
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
    
    root_logger.addHandler(console_handler)
    
    # File handler with rotation
    if settings.logging.file:
        file_handler = logging.handlers.RotatingFileHandler(
            settings.logging.file,
            maxBytes=_parse_file_size(settings.logging.max_file_size),
            backupCount=settings.logging.backup_count
        )
        file_handler.setLevel(getattr(logging, settings.logging.level))
        
        if settings.logging.enable_structured_logging:
            file_handler.setFormatter(StructuredFormatter())
        else:
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
        
        root_logger.addHandler(file_handler)
    
    # Configure structlog if enabled
    if settings.logging.enable_structured_logging:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    # Configure loguru for enhanced logging features
    loguru_logger.remove()  # Remove default handler
    
    # Add loguru console handler
    loguru_logger.add(
        sys.stdout,
        level=settings.logging.level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # Add loguru file handler
    if settings.logging.file:
        loguru_file = settings.logging.file.replace('.log', '_loguru.log')
        loguru_logger.add(
            loguru_file,
            level=settings.logging.level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation=settings.logging.max_file_size,
            retention=settings.logging.backup_count,
            compression="zip"
        )
    
    # Suppress noisy third-party loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('scrapy').setLevel(logging.WARNING)
    
    logging.info("Logging configuration initialized successfully")


def get_logger(name: str, **context) -> ETLLoggerAdapter:
    """Get configured logger with ETL context"""
    base_logger = logging.getLogger(name)
    return ETLLoggerAdapter(base_logger, context)


def get_structlog_logger(name: str) -> structlog.BoundLogger:
    """Get structlog logger for advanced structured logging"""
    return structlog.get_logger(name)


def get_loguru_logger():
    """Get loguru logger for enhanced features"""
    return loguru_logger


def _parse_file_size(size_str: str) -> int:
    """Parse file size string to bytes"""
    size_str = size_str.upper().strip()
    
    if size_str.endswith('KB'):
        return int(size_str[:-2]) * 1024
    elif size_str.endswith('MB'):
        return int(size_str[:-2]) * 1024 * 1024
    elif size_str.endswith('GB'):
        return int(size_str[:-2]) * 1024 * 1024 * 1024
    else:
        # Assume bytes
        return int(size_str)


class LoggingContext:
    """Context manager for logging with additional context"""
    
    def __init__(self, logger: ETLLoggerAdapter, operation: str, **context):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.operation}", extra=self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(f"Completed {self.operation}", extra={
                **self.context,
                'duration_seconds': duration,
                'status': 'success'
            })
        else:
            self.logger.error(f"Failed {self.operation}", extra={
                **self.context,
                'duration_seconds': duration,
                'status': 'error',
                'error_type': exc_type.__name__,
                'error_message': str(exc_val)
            })


class MetricsLogger:
    """Logger for metrics and monitoring"""
    
    def __init__(self, logger: ETLLoggerAdapter):
        self.logger = logger
        self.metrics = {}
    
    def increment(self, metric: str, value: int = 1, **tags):
        """Increment a counter metric"""
        key = f"{metric}_{hash(frozenset(tags.items()))}"
        self.metrics[key] = self.metrics.get(key, 0) + value
        
        self.logger.info(f"Metric: {metric}", extra={
            'metric_type': 'counter',
            'metric_name': metric,
            'value': value,
            'tags': tags
        })
    
    def gauge(self, metric: str, value: float, **tags):
        """Set a gauge metric"""
        self.logger.info(f"Metric: {metric}", extra={
            'metric_type': 'gauge',
            'metric_name': metric,
            'value': value,
            'tags': tags
        })
    
    def histogram(self, metric: str, value: float, **tags):
        """Record a histogram metric"""
        self.logger.info(f"Metric: {metric}", extra={
            'metric_type': 'histogram',
            'metric_name': metric,
            'value': value,
            'tags': tags
        })
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics"""
        return {
            'total_metrics': len(self.metrics),
            'metrics': self.metrics.copy()
        }


# Initialize logging on module import
if not settings.testing:
    setup_logging()


# Example usage and testing
if __name__ == "__main__":
    # Test logging configuration
    logger = get_logger(__name__, pipeline_id="test_run", component="logging_test")
    
    logger.info("Testing logging configuration")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    # Test performance logging
    logger.start_timer()
    import time
    time.sleep(0.1)
    logger.log_performance("test_operation", records_processed=100, errors=0)
    
    # Test data quality logging
    logger.log_data_quality("completeness_check", "pass", {
        "total_records": 1000,
        "complete_records": 950,
        "completeness_rate": 0.95
    })
    
    # Test extraction logging
    logger.log_extraction("test_source", 500, 5)
    
    # Test LLM logging
    logger.log_llm_processing("llama3:8b", "keyword_extraction", 150, 2500)
    
    # Test context manager
    with LoggingContext(logger, "test_operation", source="test") as ctx:
        time.sleep(0.05)
        logger.info("Operation in progress")
    
    # Test metrics logger
    metrics = MetricsLogger(logger)
    metrics.increment("companies_processed", 10, source="acra")
    metrics.gauge("data_quality_score", 0.85, table="companies")
    metrics.histogram("processing_time_ms", 1500, operation="llm_enrichment")
    
    print("Logging test completed successfully!")
