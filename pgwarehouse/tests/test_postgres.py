import os
import glob
import shutil

import testing.postgresql
import pytest
import psycopg2

from pgwarehouse.pgwarehouse import PGWarehouse

def dbsetup(postgresql):
    conn = psycopg2.connect(**postgresql.dsn())
    with conn.cursor() as cursor:
        with open(os.path.join(os.path.dirname(__file__), 'pg_setup.sql')) as f:
            cursor.execute(f.read())

        for path in glob.glob(os.path.join(os.path.dirname(__file__), 'data', '*.csv')):
            table = os.path.basename(path).split('.')[0]
            print(f"Loading CSV into table {table}")
            with open(path) as f:
                columns = f.readline().strip().split(",")
                cursor.copy_from(f, table, sep=',', columns=columns)
    conn.commit()

Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True,
                                                  on_initialized=dbsetup)

@pytest.fixture
def postgresql():
    return Postgresql()

# Use below to connect to an existing Postgres server to make it easy to examine results
# PGConf = None
# @pytest.fixture
# def new_postgresql():
#     global PGConf
#     if PGConf is None:
#         class MyConf:
#             def dsn(self):
#                 return {'host':'127.0.0.1', 'database':'pgware_test','user':'scottp','port':5432}
#         PGConf = MyConf()
#         dbsetup(PGConf)
#     return PGConf

@pytest.fixture
def connection(postgresql):
    client = psycopg2.connect(**postgresql.dsn())
    client.set_session(autocommit=True)

    os.environ['PGHOST'] = postgresql.dsn()['host']
    os.environ['PGDATABASE'] = postgresql.dsn()['database']
    os.environ['PGUSER'] = postgresql.dsn()['user']
    os.environ['PGPORT'] = str(postgresql.dsn()['port'])
    print(postgresql.dsn())
    os.environ['PGPASSWORD'] = ""

    return client
    #Postgresql.clear_cache()

@pytest.fixture
def out_dir():
    dir = os.path.join(os.path.dirname(__file__), 'output')
    shutil.rmtree(dir)
    os.makedirs(dir, exist_ok=True)
    return dir

@pytest.fixture
def ch_env():
    assert 'CLICKHOUSE_HOST' in os.environ
    return None

def drop_table(connection, table):
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

def table_size(connection, table) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f"select count(*) from {table}")
        return cursor.fetchone()[0]

def line_count(path) -> int:
    return len(list(open(path).readlines()))

def test_list(connection, out_dir, ch_env):
    pgw = PGWarehouse(command='list', table=None,
            data_dir=out_dir, backend_type='clickhouse', debug=True)
    pgw = PGWarehouse(command='listwh', table=None,
            data_dir=out_dir, backend_type='clickhouse', debug=True)
    
def test_extract(connection, out_dir, ch_env):
    for table in ['local_parks','my_orders']:
        pgw = PGWarehouse(command='extract', table=table,
                    data_dir=out_dir, backend_type='clickhouse', debug=True)
        assert os.path.exists(os.path.join(out_dir, table + "_data"))
        assert len(glob.glob(os.path.join(out_dir, table+"_data", "*"))) > 0

def test_extract_load(connection, out_dir, ch_env):
    for table in ['local_parks','users10','my_orders']:
        path = os.path.join(os.path.dirname(__file__), 'data', table+".csv")
        pgw = PGWarehouse(command='extract', table=table,
                    data_dir=out_dir, backend_type='clickhouse', debug=True)
        pgw.backend._drop_table(table)
        pgw = PGWarehouse(command='load', table=table,
                    data_dir=out_dir, backend_type='clickhouse', debug=True)
        assert pgw.count_warehouse_table(table) == table_size(connection, table)

def test_reload(connection, out_dir):
    table = 'users10'
    pgw = PGWarehouse(command='extract', table=table,
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    pgw.backend._drop_table(table)
    pgw = PGWarehouse(command='load', table=table,
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    pgw = PGWarehouse(command='reload', table=table,
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    assert pgw.count_warehouse_table(table) == table_size(connection, table)

def test_basic_sync(connection, out_dir):
    pgw = PGWarehouse(command='list',
                data_dir=out_dir, backend_type='clickhouse')
    
    for table in ['local_parks','users10','my_orders']:
        pgw.backend._drop_table(table)
        pgw = PGWarehouse(command='sync', table=table,
                    data_dir=out_dir, backend_type='clickhouse', debug=True)
        assert pgw.count_warehouse_table(table) == table_size(connection, table)

def test_incremental_sync(connection, out_dir):
    pgw = PGWarehouse(command='list',
                data_dir=out_dir, backend_type='clickhouse')
    
    table = 'users10'
    pgw.backend._drop_table(table)
    pgw = PGWarehouse(command='sync', table=table,
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    orig_size = table_size(connection, table)
    assert pgw.count_warehouse_table(table) == orig_size

    # Add some new records
    with connection.cursor() as cursor:
        cursor.execute(f"insert into {table} (name,email,age) VALUES ('sean hannity','sean@foxnews.com', 55)")
        cursor.execute(f"insert into {table} (name,email,age) VALUES ('laura ingraham','laura@foxnews.com', 45)")

    assert table_size(connection, table) == (orig_size+2)

    # Resync
    pgw = PGWarehouse(command='sync', table=table,
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    assert pgw.count_warehouse_table(table) == (orig_size+2)


def test_last_modified_sync(connection, out_dir):
    pgw = PGWarehouse(command='list',
                data_dir=out_dir, backend_type='clickhouse')
    
    table = 'my_orders'
    pgw.backend._drop_table(table)
    pgw = PGWarehouse(command='sync', table=table,
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    orig_size = table_size(connection, table)
    assert pgw.count_warehouse_table(table) == orig_size

    rows = pgw.backend._query_table(table, ['id','order_amount'], "id in (18,19)")
    for row in rows:
        assert int(row[0]) != int(row[1])

    # Update some records
    with connection.cursor() as cursor:
        cursor.execute(f"update {table} set order_updated = now(), order_amount=18 where id = 18")
        cursor.execute(f"update {table} set order_updated = now(), order_amount=19 where id = 19")
        cursor.execute(f"insert into {table} (order_updated, order_amount) VALUES (now(), 9999.9)")

    # Resync
    pgw = PGWarehouse(command='sync', table=table, last_modified='order_updated',
                data_dir=out_dir, backend_type='clickhouse', debug=True)
    assert pgw.count_warehouse_table(table) == (orig_size+1)

    rows = pgw.backend._query_table(table, ['id','order_amount'], "id in (18,19)")
    for row in rows:
        assert int(row[0]) == int(row[1])

    rows = pgw.backend._query_table(table, ['id','order_amount'], "order_amount = 9999.9")
    assert len(list(rows)) == 1

