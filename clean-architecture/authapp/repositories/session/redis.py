from logging import Logger
from authapp.repositories.session.abstract import AbstractSessionRepository


class RedisSessionRepository(AbstractSessionRepository):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        logger: Logger,
    ):
        raise NotImplementedError("This class is not implemented yet.")

    def exist_session(self, session_uuid: str) -> bool:
        ...

    def get_session_user_uuid(self, session_uuid: str) -> str:
        ...

    def create_session(self, user_uuid: str) -> str:
        ...

    def delete_session(self, session_uuid: str) -> bool:
        ...
