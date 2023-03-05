Command line:

    pgwarehouse list    - list tables from Postgres
    pgwarehouse listwh  - list tables from the warehouse
    pgwarehouse extract <table> - Extract the table from PG into local CSV files
    pgwarehouse load <table> - Load the table into the warehouse

    pgwarehouse sync <table> - Extract and load a table
    pgwarehouse resync <table> - Drops the table from the warehouse and re-syncs it
    pgwarehouse update <table>

If you supply `all` for the table name then 

## The config file

By default you place configure in `./pgwarehouse_conf.yaml`:

    postgres:
        pghost: (defaults to $PGHOST)
        pgdatabase: (defaults to $PGDATABASE
        pguser: (defaults to $PGUSER)
        pgpassword: (defaults to $PGPASSWORD)
        pgschema: (default to 'public')

    warehouse:
        backend: (clickhouse|snowflake)
        clickhouse_host: (defaults to $CLICKHOUSE_HOST)
        clickhouse_database: (defaults to $CLICKHOUSE_DATABASE)
        clickhouse_user: (defaults to $CLICKHOUSE_USER)
        clickhouse_password: (defaults to $CLICKHOUSE_PWD)
        snowsql_account: $SNOWSQL_ACCOUNT
        snowsql_database: $SNOWSQL_DATABASE
        snowsql_schema: $SNOWSQL_SCHEMA
        snowsql_warehouse: $SNOWSQL_WAREHOUSE
        snowsql_user: $SNOWSQL_USER
        snowsql_pwd: $SNOWSQL_PWD

    tables:
        - table1
        - table2
        - table3:
            skip: true
        - table4:   
            reload: true
        - table5:
            upsert: true
            last_modified: updated_at

