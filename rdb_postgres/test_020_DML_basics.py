import psycopg2
import pytest

import const

TABLE_NAME = "test_table"
ITEMS = [
    (1, "ichiro", 10),
    (2, "jiro", 20),
    (3, "saburo", 30),
    (4, "shiro", 40),
    (5, "goro", 50),
]


@pytest.mark.run(order=1)
def test_create_fresh_table():
    drop_table_query = f"DROP TABLE IF EXISTS {TABLE_NAME};"
    create_table_query = f"""
        CREATE TABLE {TABLE_NAME} (
            id serial PRIMARY KEY,
            name VARCHAR (100),
            age INT
        );
    """

    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)


@pytest.mark.run(order=10)
def test_insert():
    query = f"INSERT INTO {TABLE_NAME}(name, age) VALUES (%s, %s);"

    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            for _id, name, age in ITEMS:
                cursor.execute(query, (name, age))


@pytest.mark.run(order=20)
def test_select_all_columns():
    query = f"SELECT * FROM {TABLE_NAME};"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
    assert ITEMS == records


@pytest.mark.run(order=21)
def test_select_specified_columns():
    query = f"SELECT name, age FROM {TABLE_NAME};"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()

    items = [(name, age) for (_, name, age) in ITEMS]
    assert items == records


@pytest.mark.run(order=22)
def test_select_where():
    query = f"SELECT * FROM {TABLE_NAME} WHERE age > 30;"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()

    items = [(id_, name, age) for (id_, name, age) in ITEMS if age > 30]
    assert items == records


@pytest.mark.run(order=23)
def test_select_orderby():
    query = f"SELECT * FROM {TABLE_NAME} ORDER BY name;"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()

    items = sorted(ITEMS, key=lambda x: x[1])
    assert items == records


@pytest.mark.run(order=30)
def test_update():
    update_query = f"UPDATE {TABLE_NAME} SET age = 99 WHERE name = 'shiro';"
    select_query = f"SELECT * FROM {TABLE_NAME} WHERE name = 'shiro';"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_query)
            cursor.execute(select_query)
            record = cursor.fetchone()
    assert record == (4, "shiro", 99)


@pytest.mark.run(order=40)
def test_delete():
    delete_query = f"DELETE FROM {TABLE_NAME} WHERE name = 'shiro';"
    select_query = f"SELECT * FROM {TABLE_NAME};"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(delete_query)
            cursor.execute(select_query)
            records = cursor.fetchall()

    items = [(id_, name, age) for (id_, name, age) in ITEMS if name != "shiro"]
    assert records == items


@pytest.mark.run(order=41)
def test_delete_all():
    delete_query = f"DELETE FROM {TABLE_NAME};"
    select_query = f"SELECT * FROM {TABLE_NAME};"
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(delete_query)
            cursor.execute(select_query)
            records = cursor.fetchall()

    assert records == []
