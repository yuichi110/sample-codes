from pydantic import BaseModel


class UserSchema(BaseModel):
    id: str
    username: str
    email: str
    hashed_password: str


class UserSchemaWithoutPassword(BaseModel):
    id: str
    username: str
    email: str
