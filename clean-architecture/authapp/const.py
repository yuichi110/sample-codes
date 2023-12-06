from typing import Final

# log level
LOG_LEVEL_DEBUG = "debug"
LOG_LEVEL_INFO = "info"
LOG_LEVEL_WARNING = "warning"
LOG_LEVEL_ERROR = "error"
LOG_LEVEL_CRITICAL = "critical"

# user repository db types
USER_REPOSITORY_TYPE_MOCK: Final[str] = "mock"
USER_REPOSITORY_TYPE_POSTGRES: Final[str] = "postgres"
USER_REPOSITORY_TYPE_ORACLE: Final[str] = "oracle"

# session repository cache types
SESSION_REPOSITORY_TYPE_MOCK: Final[str] = "mock"
SESSION_REPOSITORY_TYPE_REDIS: Final[str] = "redis"
