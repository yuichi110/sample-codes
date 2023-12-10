import psycopg2
import pytest

import const

TABLE_NAME = "test_table"


def issue_query(query, has_return=False):
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("BEGIN;")
                cursor.execute(query)
                if has_return:
                    records = cursor.fetchall()
                cursor.execute("COMMIT;")
            except Exception as e:
                cursor.execute("ROLLBACK;")
                raise e
    if has_return:
        return records


@pytest.mark.run(order=1)
def test_drop_table_ifexist():
    query = f"DROP TABLE IF EXISTS {TABLE_NAME};"
    issue_query(query)


@pytest.mark.run(order=2)
def test_get_table_names():
    query = "SELECT table_name FROM information_schema.tables;"
    records = issue_query(query, has_return=True)
    assert len(records) != 0


@pytest.mark.run(order=10)
def test_check_table_notexist():
    query = f"""
        SELECT NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = '{TABLE_NAME}'
        );
    """
    result = issue_query(query, True)
    assert result[0]


@pytest.mark.run(order=20)
def test_create_table():
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id serial PRIMARY KEY,
            name VARCHAR (100),
            age INT
        );
    """
    issue_query(create_table_query)


@pytest.mark.run(order=30)
def test_create_existing_table():
    with pytest.raises(psycopg2.errors.DuplicateTable):
        create_table_query = f"""
            CREATE TABLE {TABLE_NAME} (
                id serial PRIMARY KEY,
                name VARCHAR (100),
                age INT
            );
        """
        issue_query(create_table_query)


@pytest.mark.run(order=40)
def test_check_table_exist():
    query = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = '{TABLE_NAME}'
        );
    """
    result = issue_query(query, True)
    assert result[0]


@pytest.mark.run(order=50)
def test_drop_table():
    query = f"DROP TABLE {TABLE_NAME};"
    issue_query(query)


@pytest.mark.run(order=60)
def test_drop_notexist_table():
    with pytest.raises(psycopg2.errors.UndefinedTable):
        query = f"DROP TABLE {TABLE_NAME};"
        issue_query(query)


@pytest.mark.run(order=70)
def test_check_table_notexist_again():
    query = f"""
        SELECT NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = '{TABLE_NAME}'
        );
    """
    result = issue_query(query, True)
    assert result[0]
