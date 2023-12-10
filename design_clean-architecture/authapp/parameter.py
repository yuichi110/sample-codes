import argparse
import os

import authapp.const as const


class Parameter:
    def __init__(self):
        # logging param
        self.log_level: str = const.LOG_LEVEL_INFO

        # user repo params
        self.user_repository_type: str = const.USER_REPOSITORY_TYPE_MOCK
        self.user_repository_host: str = "0.0.0.0"
        self.user_repository_port: str = "-1"
        self.user_repository_user: str = "admin"
        self.user_repository_password: str = "password"

        # session repo params
        self.session_repository_type: str = const.SESSION_REPOSITORY_TYPE_MOCK
        self.session_repository_host: str = "0.0.0.0"
        self.session_repository_port: str = "-1"
        self.session_repository_user: str = "admin"
        self.session_repository_password: str = "password"

        self._args, _ = _parser.parse_known_args()

    def load(self):
        self._load_user_repository_parameters()
        self._load_session_repository_parameters()

    def _load_logging_parameters(self):
        def set_level():
            self.log_level = self._get_arg1st_env2nd_default3rd(
                self._args.log_level,
                "LOG_LEVEL",
                self.log_level,
            )

        set_level()

    def _load_user_repository_parameters(self):
        def set_type():
            self.user_repository_type = self._get_arg1st_env2nd_default3rd(
                self._args.user_db_type,
                "USER_DB_TYPE",
                self.user_repository_type,
            )

        def set_host():
            self.user_repository_host = self._get_arg1st_env2nd_default3rd(
                self._args.user_db_host,
                "USER_DB_HOST",
                self.user_repository_host,
            )

        def set_port():
            self.user_repository_port = self._get_arg1st_env2nd_default3rd(
                self._args.user_db_port,
                "USER_DB_port",
                self.user_repository_port,
            )

        def set_user():
            self.user_repository_user = self._get_arg1st_env2nd_default3rd(
                self._args.user_db_user,
                "USER_DB_USER",
                self.user_repository_user,
            )

        def set_password():
            self.user_repository_password = self._get_arg1st_env2nd_default3rd(
                self._args.user_db_password,
                "USER_DB_PASSWORD",
                self.user_repository_password,
            )

        set_type()
        set_host()
        set_port()
        set_user()
        set_password()

    def _load_session_repository_parameters(self):
        def set_type():
            self.session_repository_type = self._get_arg1st_env2nd_default3rd(
                self._args.session_cache_type,
                "SESSION_CACHE_TYPE",
                self.session_repository_type,
            )

        def set_host():
            self.session_repository_host = self._get_arg1st_env2nd_default3rd(
                self._args.session_cache_host,
                "SESSION_CACHE_HOST",
                self.session_repository_host,
            )

        def set_port():
            self.session_repository_port = self._get_arg1st_env2nd_default3rd(
                self._args.session_cache_port,
                "SESSION_CACHE_PORT",
                self.session_repository_port,
            )

        def set_user():
            self.session_repository_user = self._get_arg1st_env2nd_default3rd(
                self._args.session_cache_user,
                "SESSION_CACHE_USER",
                self.session_repository_user,
            )

        def set_password():
            self.session_repository_password = self._get_arg1st_env2nd_default3rd(
                self._args.session_cache_password,
                "SESSION_CACHE_PASSWORD",
                self.session_repository_password,
            )

        set_type()
        set_host()
        set_port()
        set_user()
        set_password()

    def _get_arg1st_env2nd_default3rd(
        self,
        arg: str,
        env: str,
        default,
    ) -> str:
        if arg:
            return arg
        if env in os.environ:
            return os.environ[env]
        return str(default)


def _get_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TZ Userapp.",
    )
    # logging
    parser.add_argument(
        "--log_level",
        help="log level. [debug|info|warning|error|critical]",
    )

    # database
    parser.add_argument(
        "--user_db_type",
        help="DB type. [MOCK|POSTGRES|ORACLE]",
    )
    parser.add_argument(
        "--user_db_host",
        help="DB host",
    )
    parser.add_argument(
        "--user_db_port",
        help="DB port",
    )
    parser.add_argument(
        "--user_db_user",
        help="DB username",
    )
    parser.add_argument(
        "--user_db_password",
        help="DB password",
    )

    # cache
    parser.add_argument(
        "--session_cache_type",
        help="Cache type. [MOCK|REDIS]",
    )
    parser.add_argument(
        "--session_cache_host",
        help="Cache host",
    )
    parser.add_argument(
        "--session_cache_port",
        help="Cache port",
    )
    parser.add_argument(
        "--session_cache_user",
        help="Cache username",
    )
    parser.add_argument(
        "--session_cache_password",
        help="Cache password",
    )
    return parser


_parser: argparse.ArgumentParser = _get_arg_parser()
