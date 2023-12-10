import re
import uuid


def get_random_uuid() -> str:
    random_uuid = uuid.uuid4()
    return str(random_uuid)


def is_password_strength_ok(raw_password: str) -> bool:
    return True


def get_hashed_password(raw_password: str) -> str:
    # this is just an easy-to-understand example
    # please use bcrypt and salt etc for real world.
    return "hashed_" + raw_password


def is_valid_username(name):
    regex = r"^[a-zA-Z0-9]{3,8}$"
    return bool(re.match(regex, name))


def is_valid_email(email):
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return bool(re.match(email_regex, email))
