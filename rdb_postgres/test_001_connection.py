import psycopg2
import pytest

import const


def test_connection():
    conn = psycopg2.connect(
        host=const.HOST,
        port=const.PORT,
        user=const.USER,
        password=const.PASSWORD,
        database=const.DATABASE,
    )
    assert conn.closed == 0  # open
    conn.close()
    assert conn.closed != 0  # close

    psycopg2.connect(**const.PG_PARAMS).close()


def test_connection_notexist():
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(
            host=const.HOST,
            port=9999,
            user=const.USER,
            password=const.PASSWORD,
            database=const.DATABASE,
        )


def test_connection_with():
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        assert conn.closed == 0
