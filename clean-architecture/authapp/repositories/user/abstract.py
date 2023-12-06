from abc import ABC, abstractmethod
from authapp.models.user import UserSchema


class AbstractUserRepository(ABC):
    @abstractmethod
    def get_users(self) -> list[UserSchema]:
        ...

    @abstractmethod
    def get_user_by_id(self, uuid: str) -> UserSchema:
        ...

    @abstractmethod
    def get_user_by_username(self, username: str) -> UserSchema:
        ...

    @abstractmethod
    def get_user_by_email(self, email: str) -> UserSchema:
        ...

    @abstractmethod
    def create_user_atomically(
        self,
        username: str,
        email: str,
        password: str,
    ) -> None:
        ...

    @abstractmethod
    def modify_user_atomically(
        self,
        username: str,
        email: str,
        password: str,
    ) -> None:
        ...
