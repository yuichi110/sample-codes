import psycopg2
import pytest

import const

#  ORDER_TABLE <-- USER_TABLE
#       |
#       V
#  ORDER_DETAILS_TABLE <-- ITEM_TABLE
#
# 1. ichiro bought 5 apples, 1 banana, 3 orange
# 2. jiro bought 3 apples, 1 grape, 1 melon
# 3. ichiro bought 3 apples

table_customers = {
    "name": "test_customers",
    "items": [
        # id, name
        (1, "ichiro"),
        (2, "jiro"),
        (3, "saburo"),
        (4, "shiro"),
        (5, "goro"),
    ],
    "drop_query": "DROP TABLE IF EXISTS test_customers CASCADE;",
    "create_query": """
        CREATE TABLE test_customers (
            id INT PRIMARY KEY,
            name VARCHAR (100)
        );
    """,
    "insert_query": "INSERT INTO test_customers(id, name) VALUES (%s, %s);",
}

table_items = {
    "name": "test_items",
    "items": [
        # id, name, price
        (1, "apple", 100),
        (2, "banama", 200),
        (3, "orange", 100),
        (4, "grape", 500),
        (5, "melon", 1000),
    ],
    "drop_query": "DROP TABLE IF EXISTS test_items CASCADE;",
    "create_query": """
        CREATE TABLE test_items (
            id INT PRIMARY KEY,
            name VARCHAR (100),
            price INT
        );
    """,
    "insert_query": "INSERT INTO test_items (id, name, price) VALUES (%s, %s, %s);",
}

table_orders = {
    "name": "test_orders",
    "items": [
        # id, customer_id, date
        (1, 1, "2023-12-31 15:30:00"),
        (2, 2, "2023-12-31 18:00:00"),
        (3, 1, "2023-12-31 18:30:00"),
    ],
    "drop_query": "DROP TABLE IF EXISTS test_orders CASCADE;",
    "create_query": """
        CREATE TABLE test_orders (
            id INT PRIMARY KEY,
            customer_id INT REFERENCES test_customers(id),
            date TIMESTAMP
        );
    """,
    "insert_query": "INSERT INTO test_orders (id, customer_id, date) VALUES (%s, %s, %s);",
}

table_order_details = {
    "name": "test_order_details",
    "items": [
        # id, order_id, item_id, quantity
        (1, 1, 1, 5),
        (2, 1, 2, 1),
        (3, 1, 3, 3),
        (4, 2, 1, 3),
        (5, 2, 4, 1),
        (6, 2, 5, 1),
        (7, 3, 1, 3),
    ],
    "drop_query": "DROP TABLE IF EXISTS test_order_details CASCADE;",
    "create_query": """
        CREATE TABLE test_order_details (
            id INT PRIMARY KEY,
            order_id INT REFERENCES test_orders(id),
            item_id INT REFERENCES test_items(id),
            quantity INT
        );
    """,
    "insert_query": """
        INSERT INTO test_order_details
            (id, order_id, item_id, quantity)
            VALUES (%s, %s, %s, %s);
    """,
}


@pytest.mark.run(order=1)
def test_create_fresh_table():
    tables = [table_customers, table_items, table_orders, table_order_details]
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute("BEGIN;")
            for table in tables:
                drop_table_query = table["drop_query"]
                create_table_query = table["create_query"]
                insert_query = table["insert_query"]
                # prepare table
                cursor.execute(drop_table_query)
                cursor.execute(create_table_query)
                # insert
                for row_data in table["items"]:
                    cursor.execute(insert_query, row_data)
            cursor.execute("COMMIT;")


@pytest.mark.run(order=10)
def test_join1():
    query = """
        SELECT test_orders.id, test_customers.name
            FROM test_orders
            JOIN test_customers ON test_orders.customer_id = test_customers.id;
        """

    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    print(results)
    assert set(results) == set(
        [
            (1, "ichiro"),
            (2, "jiro"),
            (3, "ichiro"),
        ]
    )


@pytest.mark.run(order=10)
def test_join2():
    query = """
        SELECT test_orders.id, test_customers.name, test_order_details.item_id, test_order_details.quantity
            FROM test_orders
            JOIN test_customers ON test_orders.customer_id = test_customers.id
            JOIN test_order_details ON test_orders.id = test_order_details.order_id;
        """

    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    print(results)
    assert set(results) == set(
        [
            (1, "ichiro", 1, 5),
            (1, "ichiro", 2, 1),
            (1, "ichiro", 3, 3),
            (2, "jiro", 1, 3),
            (2, "jiro", 4, 1),
            (2, "jiro", 5, 1),
            (3, "ichiro", 1, 3),
        ]
    )


@pytest.mark.run(order=10)
def test_join3():
    query = """
        SELECT test_orders.id, test_customers.name, test_items.name, test_order_details.quantity
            FROM test_orders
            JOIN test_customers ON test_orders.customer_id = test_customers.id
            JOIN test_order_details ON test_orders.id = test_order_details.order_id
            JOIN test_items ON test_items.id = test_order_details.item_id
        ;
        """

    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    print(results)
    assert set(results) == set(
        [
            (1, "ichiro", "apple", 5),
            (1, "ichiro", "banama", 1),
            (1, "ichiro", "orange", 3),
            (2, "jiro", "apple", 3),
            (2, "jiro", "grape", 1),
            (2, "jiro", "melon", 1),
            (3, "ichiro", "apple", 3),
        ]
    )


@pytest.mark.run(order=10)
def test_left_join():
    query = """
        SELECT test_customers.name, test_orders.id
            FROM test_customers
            LEFT JOIN test_orders ON test_customers.id = test_orders.customer_id
        ;
    """
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    print(results)
    assert set(results) == set(
        [
            ("ichiro", 1),
            ("jiro", 2),
            ("ichiro", 3),
            ("goro", None),
            ("shiro", None),
            ("saburo", None),
        ]
    )


@pytest.mark.run(order=10)
def test_right_join():
    query = """
        SELECT test_customers.name, test_orders.id
            FROM test_customers
            RIGHT JOIN test_orders ON test_customers.id = test_orders.customer_id
        ;
    """
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    print(results)
    assert set(results) == set(
        [
            ("ichiro", 1),
            ("jiro", 2),
            ("ichiro", 3),
        ]
    )


@pytest.mark.run(order=10)
def test_full_join():
    query = """
        SELECT test_customers.name, test_orders.id
            FROM test_customers
            FULL JOIN test_orders ON test_customers.id = test_orders.customer_id
        ;
    """
    with psycopg2.connect(**const.PG_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    print(results)
    assert set(results) == set(
        [
            ("ichiro", 1),
            ("jiro", 2),
            ("ichiro", 3),
            ("goro", None),
            ("shiro", None),
            ("saburo", None),
        ]
    )
