from logging import Logger
import pandas as pd

from authapp.exceptions import ClientException
from authapp.repositories.user.abstract import AbstractUserRepository
from authapp.models.user import UserSchema
from authapp.util import get_hashed_password, get_random_uuid

_INITIAL_USERS = [
    UserSchema(
        id="34b8584f-d79f-4b50-b20f-d0abbc87676e",
        username="yuichi",
        email="iyuichi@vmware.com",
        hashed_password=get_hashed_password("p@ssw0rd"),
    ),
    UserSchema(
        id="f75a79c9-e054-4d17-8be8-954a66a7c571",
        username="shunsuke",
        email="shunsukeh@vmware.com",
        hashed_password=get_hashed_password("p@ssw0rd"),
    ),
]


class MockUserRepository(AbstractUserRepository):
    def __init__(self, logger: Logger):
        self._logger = logger
        self._users = pd.DataFrame([o.model_dump() for o in _INITIAL_USERS])

    def get_users(self) -> list[UserSchema]:
        list_of_dicts = self._users.to_dict(orient="records")
        return [UserSchema.model_validate(d) for d in list_of_dicts]

    def get_user_by_id(self, uuid: str) -> UserSchema:
        result = self._users[self._users["id"] == uuid]
        return self._get_user_from_result(result)

    def get_user_by_username(self, username: str) -> UserSchema:
        result = self._users[self._users["username"] == username]
        return self._get_user_from_result(result)

    def get_user_by_email(self, email: str) -> UserSchema:
        result = self._users[self._users["email"] == email]
        return self._get_user_from_result(result)

    def create_user_atomically(self, username, email, password) -> None:
        # check existance
        if len(self._users[self._users["username"] == username]) != 0:
            raise ClientException("username is already used")
        if len(self._users[self._users["email"] == email]) != 0:
            raise ClientException("email is already used")

        # ok. create
        obj = UserSchema(
            id=get_random_uuid(),
            username=username,
            email=email,
            hashed_password=get_hashed_password(password),
        )
        newline_df = pd.DataFrame([obj.model_dump()])
        self._users = pd.concat([self._users, newline_df], ignore_index=True)

    def modify_user_atomically(self, name, email, password) -> None:
        ...

    def _get_user_from_result(self, result: pd.DataFrame):
        if len(result) == 0:
            raise ClientException("user not found")
        d = result.to_dict(orient="records")[0]
        return UserSchema.model_validate(d)
