import logging
from logging import Logger

import authapp.const as const
from authapp.parameter import Parameter
from authapp.repositories.user.abstract import AbstractUserRepository
from authapp.repositories.user.mock import MockUserRepository
from authapp.repositories.user.postgres import PostgresUserRepository
from authapp.repositories.user.oracle import OracleUserRepository
from authapp.repositories.session.abstract import AbstractSessionRepository
from authapp.repositories.session.mock import MockSessionRepository
from authapp.repositories.session.redis import RedisSessionRepository
from authapp.services.auth import AuthService


class DiContainer:
    def __init__(self, parameter: Parameter):
        self._parameter = parameter

    def get_service(self) -> AuthService:
        logger = self._get_logger()
        user_repository = self._get_user_repository(logger)
        session_repository = self._get_session_repository(logger)
        return AuthService(user_repository, session_repository, logger)

    def _get_logger(self) -> Logger:
        logger = logging.getLogger()
        log_level = self._parameter.log_level.lower()
        if log_level == const.LOG_LEVEL_DEBUG:
            logger.setLevel(logging.DEBUG)
        elif log_level == const.LOG_LEVEL_INFO:
            logger.setLevel(logging.INFO)
        elif log_level == const.LOG_LEVEL_WARNING:
            logger.setLevel(logging.WARNING)
        elif log_level == const.LOG_LEVEL_ERROR:
            logger.setLevel(logging.ERROR)
        elif log_level == const.LOG_LEVEL_CRITICAL:
            logger.setLevel(logging.CRITICAL)
        else:
            raise ValueError(
                "Logging level must be [debug|info|warning|error|critical]"
            )
        return logger

    def _get_user_repository(self, logger: Logger) -> AbstractUserRepository:
        repo_type = self._parameter.user_repository_type.lower()
        if repo_type == const.USER_REPOSITORY_TYPE_MOCK.lower():
            return MockUserRepository(logger)

        host = self._parameter.user_repository_host
        port = self._get_port_int(self._parameter.user_reporitory_port)
        user = self._parameter.user_repository_user
        password = self._parameter.user_repository_password
        if repo_type == const.USER_REPOSITORY_TYPE_POSTGRES.lower():
            return PostgresUserRepository(host, port, user, password, logger)
        if repo_type == const.USER_REPOSITORY_TYPE_ORACLE.lower():
            return OracleUserRepository(host, port, user, password, logger)

        raise ValueError("User DB type must be [mock|postgres|oracle]")

    def _get_session_repository(
        self,
        logger: Logger,
    ) -> AbstractSessionRepository:
        repo_type = self._parameter.session_repository_type.lower()
        if repo_type == const.SESSION_REPOSITORY_TYPE_MOCK.lower():
            return MockSessionRepository(logger)

        host = self._parameter.session_repository_host
        port = self._get_port_int(self._parameter.session_reporitory_port)
        user = self._parameter.session_repository_user
        password = self._parameter.session_repository_password
        if repo_type == const.SESSION_REPOSITORY_TYPE_REDIS.lower():
            return RedisSessionRepository(host, port, user, password, logger)

        raise ValueError("Session cache type must be [mock|redis]")

    def _get_port_int(num: str) -> int:
        try:
            int_num = int(num)
        except Exception:
            raise ValueError("Port number must be integer.")
        if 0 <= int_num <= 65535:
            return int_num
        raise ValueError("Port number must be within 0-65535")
