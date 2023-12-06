import logging
from authapp.repositories.session.mock import MockSessionRepository


def test_all():
    # should create tests for all methods
    repo = MockSessionRepository(logging.getLogger())
    user_uuid = "this_is_a_test"
    session_uuid = repo.create_session(user_uuid)
    assert repo.exist_session(session_uuid)
    assert repo.get_session_user_uuid(session_uuid) == user_uuid
