import psycopg2
import pytest
import threading
import time

import const

#  ORDER_TABLE <-- USER_TABLE
#       |
#       V
#  ORDER_DETAILS_TABLE <-- ITEM_TABLE
#
# ichiro bought 5 apples, 1 banana, 3 orange
# jiro bought 3 apples, 1 grape, 1 melon

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
    ],
    "drop_query": "DROP TABLE IF EXISTS test_orders CASCADE;",
    "create_query": """
        CREATE TABLE test_orders (
            id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES test_customers(id),
            date TIMESTAMP
        );
    """,
    "insert_query": """
        INSERT INTO test_orders (customer_id, date)
            VALUES (%s, %s)
            RETURNING id
        ;
    """,
}

table_order_details = {
    "name": "test_order_details",
    "items": [
        # id, order_id, item_id, quantity
    ],
    "drop_query": "DROP TABLE IF EXISTS test_order_details CASCADE;",
    "create_query": """
        CREATE TABLE test_order_details (
            id SERIAL PRIMARY KEY,
            order_id INT REFERENCES test_orders(id),
            item_id INT REFERENCES test_items(id),
            quantity INT
        );
    """,
    "insert_query": """
        INSERT INTO test_order_details
            (order_id, item_id, quantity)
            VALUES (%s, %s, %s)
            RETURNING id
        ;
    """,
}

select_query = """
    SELECT test_orders.id, test_customers.name, test_items.name, test_order_details.quantity
        FROM test_orders
        JOIN test_customers ON test_orders.customer_id = test_customers.id
        JOIN test_order_details ON test_orders.id = test_order_details.order_id
        JOIN test_items ON test_items.id = test_order_details.item_id
    ;
    """


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


@pytest.mark.run(order=1)
def test_select_for_update():
    def work1():
        ...

    def work2():
        ...

    t1 = threading.Thread(target=work1)
    t2 = threading.Thread(target=work2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
