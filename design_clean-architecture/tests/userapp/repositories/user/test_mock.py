import logging
import pytest
from authapp.repositories.user.mock import MockUserRepository
from authapp.exceptions import ClientException
from authapp.util import get_hashed_password


def test_all():
    # should create tests for all methods
    repo = MockUserRepository(logging.getLogger())
    assert len(repo.get_users()) == 2

    user1 = repo.get_user_by_id("34b8584f-d79f-4b50-b20f-d0abbc87676e")
    assert user1.username == "yuichi"

    user2 = repo.get_user_by_username("yuichi")
    assert user2.username == "yuichi"

    user3 = repo.get_user_by_email("iyuichi@vmware.com")
    assert user3.username == "yuichi"

    with pytest.raises(ClientException) as exc_info:
        repo.get_user_by_id("not_exist_uuid")
    assert exc_info.type is ClientException

    with pytest.raises(ClientException) as exc_info:
        repo.get_user_by_username("taro")
    assert exc_info.type is ClientException

    with pytest.raises(ClientException) as exc_info:
        repo.get_user_by_email("taro@vmware.com")
    assert exc_info.type is ClientException


def test_create():
    repo = MockUserRepository(logging.getLogger())
    username = "tanzu"
    email = "tanzu@vmware.com"
    password = "weak_password"
    repo.create_user_atomically(username, email, password)

    user1 = repo.get_user_by_username("tanzu")
    assert user1.username == username
    assert user1.email == email
    assert user1.hashed_password == get_hashed_password(password)

    with pytest.raises(ClientException) as exc_info:
        # username already exist
        repo.create_user_atomically("tanzu", "tanzu2@vmware.com", password)
    assert exc_info.type is ClientException

    with pytest.raises(ClientException) as exc_info:
        # email already exist
        repo.create_user_atomically("tanzu2", "tanzu@vmware.com", password)
    assert exc_info.type is ClientException
