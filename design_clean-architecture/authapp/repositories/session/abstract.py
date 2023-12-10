from abc import ABC, abstractmethod


class AbstractSessionRepository(ABC):
    @abstractmethod
    def exist_session(self, session_uuid: str) -> bool:
        ...

    @abstractmethod
    def get_session_user_uuid(self, session_uuid: str) -> str:
        ...

    @abstractmethod
    def create_session(self, user_uuid: str) -> str:
        ...

    @abstractmethod
    def delete_session(self, session_uuid: str) -> str:
        ...
