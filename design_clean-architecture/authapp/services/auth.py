from logging import Logger

import authapp.util as util
from authapp.exceptions import ClientException
from authapp.repositories.user.abstract import AbstractUserRepository
from authapp.repositories.session.abstract import AbstractSessionRepository
from authapp.models.user import UserSchema, UserSchemaWithoutPassword
from authapp.models.httpbody import SigninBody, SignupBody
from authapp.util import get_hashed_password


class AuthService:
    def __init__(
        self,
        user_repo: AbstractUserRepository,
        session_repo: AbstractSessionRepository,
        logger: Logger,
    ):
        self._user_repo = user_repo
        self._session_repo = session_repo
        self._logger = logger

    def list_users(self) -> list[UserSchemaWithoutPassword]:
        # debug purpose for sample app.
        users: list[UserSchemaWithoutPassword] = []
        for user in self._user_repo.get_users():
            d = user.model_dump()
            users.append(UserSchemaWithoutPassword.model_validate(d))
        return users

    def get_user(
        self,
        username: str,
        cookies: dict,
    ) -> UserSchemaWithoutPassword:
        try:
            session_uuid = cookies["session"]
            session_user_uuid = self._session_repo.get_session_user_uuid(session_uuid)
            user: UserSchema = self._user_repo.get_user_by_username(username)
            if session_user_uuid != user.id:
                raise ClientException
        except Exception:
            raise ClientException("authentication error")
        return UserSchemaWithoutPassword.model_validate(user.model_dump())

    def signup(self, signup_obj: SignupBody) -> None:
        # validate
        username = signup_obj.username.strip()
        email = signup_obj.email.strip()
        raw_password1 = signup_obj.password1.strip()
        raw_password2 = signup_obj.password2.strip()
        if not util.is_valid_username(username):
            raise ClientException("username format invalid")
        if not util.is_valid_email(email):
            raise ClientException("email format invalid")
        if raw_password1 != raw_password2:
            raise ClientException("password mismatch")
        if not util.is_password_strength_ok(raw_password1):
            raise ClientException("password is too weak")

        # create user
        self._user_repo.create_user_atomically(username, email, raw_password1)

    def signin(self, signin_obj: SigninBody) -> dict:
        username_or_email = signin_obj.username_or_email.strip()
        raw_password = signin_obj.password.strip()
        user = self._challenge_password(username_or_email, raw_password)

        # create session
        session_uuid = self._session_repo.create_session(user.id)
        cookies = {
            "session": session_uuid,
            "username": user.username,
            "user_id": user.id,
        }
        return cookies

    def signout(self, cookies: dict) -> list[str]:
        if "session" in cookies:
            session_uuid = cookies["session"]
            self._session_repo.delete_session(session_uuid)
        del_cookie_keys = ["session", "username", "user_id"]
        return del_cookie_keys

    def _challenge_password(
        self, username_or_email: str, raw_password: str
    ) -> UserSchema:
        hashed_password = get_hashed_password(raw_password)
        try:
            # check user exist
            if util.is_valid_email(username_or_email):
                email = username_or_email
                user = self._user_repo.get_user_by_email(email)
            elif util.is_valid_username(username_or_email):
                username = username_or_email
                user = self._user_repo.get_user_by_username(username)
            else:
                raise ClientException()

            # check password
            if user.hashed_password != hashed_password:
                raise ClientException()

        except ClientException:
            # hide exact fail reason for security
            raise ClientException("authentication failed")

        return user
