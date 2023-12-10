from logging import Logger
from cachetools import TTLCache
from authapp.repositories.session.abstract import AbstractSessionRepository
from authapp.util import get_random_uuid

_CACHE_SIZE = 10000
_THREE_HOUR = 60 * 60 * 3


class MockSessionRepository(AbstractSessionRepository):
    def __init__(self, logger: Logger):
        self._sessions = TTLCache(maxsize=_CACHE_SIZE, ttl=_THREE_HOUR)

    def exist_session(self, session_uuid: str) -> bool:
        return session_uuid in self._sessions

    def get_session_user_uuid(self, session_uuid: str) -> str:
        return self._sessions[session_uuid]

    def create_session(self, user_uuid: str) -> str:
        session_uuid = get_random_uuid()
        self._sessions[session_uuid] = user_uuid
        return session_uuid

    def delete_session(self, session_uuid: str) -> bool:
        if session_uuid in self._sessions:
            del self._sessions[session_uuid]
