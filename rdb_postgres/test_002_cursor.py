import psycopg2

import const


def test_cursor():
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables;")
        records = cursor.fetchall()
        assert len(records) != 0
        cursor.close()


def test_cursor_with():
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables;")
            records = cursor.fetchall()
            assert len(records) != 0
