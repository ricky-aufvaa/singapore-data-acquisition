"""
Configuration management for Singapore Company Database ETL Pipeline
Handles environment variables, database connections, and application settings
"""

import os
from typing import List, Optional
from pydantic import BaseSettings, Field, validator
from pydantic_settings import SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""
    
    model_config = SettingsConfigDict(env_prefix='DB_')
    
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    name: str = Field(default="singapore_companies", description="Database name")
    user: str = Field(default="postgres", description="Database user")
    password: str = Field(description="Database password")
    
    @property
    def url(self) -> str:
        """Generate database URL"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class LLMConfig(BaseSettings):
    """LLM configuration settings"""
    
    model_config = SettingsConfigDict(env_prefix='LLM_')
    
    ollama_host: str = Field(default="http://localhost:11434", description="Ollama server host")
    model_name: str = Field(default="llama3:8b", description="LLM model name")
    temperature: float = Field(default=0.1, description="LLM temperature")
    max_tokens: int = Field(default=500, description="Maximum tokens per request")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    
    @validator('temperature')
    def validate_temperature(cls, v):
        if not 0.0 <= v <= 2.0:
            raise ValueError('Temperature must be between 0.0 and 2.0')
        return v


class ScrapingConfig(BaseSettings):
    """Web scraping configuration"""
    
    model_config = SettingsConfigDict(env_prefix='SCRAPING_')
    
    delay: float = Field(default=1.0, description="Delay between requests in seconds")
    max_concurrent_requests: int = Field(default=10, description="Maximum concurrent requests")
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        description="User agent string"
    )
    respect_robots_txt: bool = Field(default=True, description="Respect robots.txt")
    enable_proxy_rotation: bool = Field(default=False, description="Enable proxy rotation")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    
    @validator('delay')
    def validate_delay(cls, v):
        if v < 0.1:
            raise ValueError('Delay must be at least 0.1 seconds')
        return v


class RateLimitConfig(BaseSettings):
    """Rate limiting configuration"""
    
    model_config = SettingsConfigDict(env_prefix='REQUESTS_PER_')
    
    second: int = Field(default=2, description="Requests per second")
    minute: int = Field(default=100, description="Requests per minute")
    hour: int = Field(default=5000, description="Requests per hour")


class RedisConfig(BaseSettings):
    """Redis configuration for caching"""
    
    model_config = SettingsConfigDict(env_prefix='REDIS_')
    
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")
    
    @property
    def url(self) -> str:
        """Generate Redis URL"""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class LoggingConfig(BaseSettings):
    """Logging configuration"""
    
    model_config = SettingsConfigDict(env_prefix='LOG_')
    
    level: str = Field(default="INFO", description="Log level")
    file: str = Field(default="logs/singapore_companies.log", description="Log file path")
    enable_structured_logging: bool = Field(default=True, description="Enable structured logging")
    max_file_size: str = Field(default="100MB", description="Maximum log file size")
    backup_count: int = Field(default=5, description="Number of backup log files")
    
    @validator('level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of: {valid_levels}')
        return v.upper()


class DataQualityConfig(BaseSettings):
    """Data quality thresholds and settings"""
    
    min_company_name_length: int = Field(default=2, description="Minimum company name length")
    max_company_name_length: int = Field(default=200, description="Maximum company name length")
    fuzzy_match_threshold: int = Field(default=85, description="Fuzzy matching threshold (0-100)")
    duplicate_detection_threshold: int = Field(default=90, description="Duplicate detection threshold")
    min_quality_score: float = Field(default=0.5, description="Minimum acceptable quality score")
    
    @validator('fuzzy_match_threshold', 'duplicate_detection_threshold')
    def validate_threshold(cls, v):
        if not 0 <= v <= 100:
            raise ValueError('Threshold must be between 0 and 100')
        return v


class PipelineConfig(BaseSettings):
    """ETL pipeline configuration"""
    
    batch_size: int = Field(default=1000, description="Processing batch size")
    max_workers: int = Field(default=4, description="Maximum worker threads")
    enable_parallel_processing: bool = Field(default=True, description="Enable parallel processing")
    checkpoint_interval: int = Field(default=5000, description="Checkpoint interval")
    max_errors_per_batch: int = Field(default=100, description="Maximum errors per batch")
    
    @validator('batch_size', 'max_workers', 'checkpoint_interval')
    def validate_positive(cls, v):
        if v <= 0:
            raise ValueError('Value must be positive')
        return v


class MonitoringConfig(BaseSettings):
    """Monitoring and metrics configuration"""
    
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    metrics_port: int = Field(default=8000, description="Metrics server port")
    prometheus_endpoint: str = Field(default="/metrics", description="Prometheus metrics endpoint")
    health_check_interval: int = Field(default=60, description="Health check interval in seconds")


class APIConfig(BaseSettings):
    """External API configuration"""
    
    linkedin_username: Optional[str] = Field(default=None, description="LinkedIn username")
    linkedin_password: Optional[str] = Field(default=None, description="LinkedIn password")
    crunchbase_api_key: Optional[str] = Field(default=None, description="Crunchbase API key")
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key (fallback)")


class Settings(BaseSettings):
    """Main application settings"""
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False
    )
    
    # Application settings
    debug: bool = Field(default=False, description="Debug mode")
    testing: bool = Field(default=False, description="Testing mode")
    enable_profiling: bool = Field(default=False, description="Enable performance profiling")
    
    # Component configurations
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    
    # Data source URLs
    data_sources: dict = Field(default_factory=lambda: {
        'acra': 'https://www.acra.gov.sg',
        'yellow_pages': 'https://www.yellowpages.com.sg',
        'sgx': 'https://www.sgx.com',
        'gebiz': 'https://www.gebiz.gov.sg',
        'linkedin': 'https://www.linkedin.com',
        'crunchbase': 'https://www.crunchbase.com'
    })
    
    # Industry classifications
    industries: List[str] = Field(default_factory=lambda: [
        'Technology', 'FinTech', 'Healthcare', 'E-commerce', 'Manufacturing',
        'Professional Services', 'Real Estate', 'F&B', 'Education', 'Logistics',
        'Construction', 'Retail', 'Energy', 'Media', 'Automotive', 'Agriculture',
        'Tourism', 'Government', 'Non-Profit', 'Other'
    ])
    
    # Company size categories
    company_sizes: List[str] = Field(default_factory=lambda: [
        'Micro (1-10)', 'Small (11-50)', 'Medium (51-200)', 
        'Large (201-1000)', 'Enterprise (1000+)', 'Unknown'
    ])
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize nested configs with environment variables
        self.database = DatabaseConfig()
        self.llm = LLMConfig()
        self.scraping = ScrapingConfig()
        self.rate_limit = RateLimitConfig()
        self.redis = RedisConfig()
        self.logging = LoggingConfig()
        self.data_quality = DataQualityConfig()
        self.pipeline = PipelineConfig()
        self.monitoring = MonitoringConfig()
        self.api = APIConfig()
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.debug or os.getenv('ENVIRONMENT', '').lower() in ['dev', 'development']
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return os.getenv('ENVIRONMENT', '').lower() in ['prod', 'production']
    
    def get_data_source_url(self, source_name: str) -> Optional[str]:
        """Get URL for a specific data source"""
        return self.data_sources.get(source_name.lower())
    
    def validate_configuration(self) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        # Check required database settings
        if not self.database.password:
            issues.append("Database password is required")
        
        # Check LLM configuration
        if not self.llm.ollama_host.startswith(('http://', 'https://')):
            issues.append("Invalid Ollama host URL")
        
        # Check file paths
        log_dir = os.path.dirname(self.logging.file)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                issues.append(f"Cannot create log directory: {e}")
        
        return issues


# Global settings instance
settings = Settings()

# Validate configuration on import
config_issues = settings.validate_configuration()
if config_issues and not settings.testing:
    import warnings
    for issue in config_issues:
        warnings.warn(f"Configuration issue: {issue}")
