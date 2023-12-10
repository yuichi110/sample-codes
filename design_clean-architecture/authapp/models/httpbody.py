from pydantic import BaseModel


class BlankJsonSchema(BaseModel):
    ...


class SignupBody(BaseModel):
    username: str
    email: str
    password1: str
    password2: str


class SigninBody(BaseModel):
    username_or_email: str
    password: str
