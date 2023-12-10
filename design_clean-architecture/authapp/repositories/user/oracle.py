from logging import Logger

from authapp.repositories.user.abstract import AbstractUserRepository
from authapp.models.user import UserSchema


class OracleUserRepository(AbstractUserRepository):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        logger: Logger,
    ):
        raise NotImplementedError("This class is not implemented yet.")

    def get_users(self) -> list[UserSchema]:
        ...

    def get_user_by_id(self, uuid: str) -> UserSchema:
        ...

    def get_user_by_username(self, username: str) -> UserSchema:
        ...

    def get_user_by_email(self, email: str) -> UserSchema:
        ...

    def create_user_atomically(
        self,
        username: str,
        email: str,
        password: str,
    ) -> None:
        ...

    def modify_user_atomically(
        self,
        username: str,
        email: str,
        password: str,
    ) -> None:
        ...
