# pgwarehouse - quickly sync Postgres data to your cloud warehouse

## Introduction

Postgres is an amazing, general purpose OLTP database. But it's not designed for heavy analytic (OLAP) usage. Analytic queries are much better served by a columnar store database like Snowflake or Clickhouse.

This package allows you to easily sync data from a Postgres database into a local or cloud data warehouse (currently Snowflake or Clickhouse). You can perform a one-time sync operation, or run periodic incremental syncs to keep your warehouse up to date.

## Features

* High performance by using `COPY` to move lots of data efficiently. `pgwarehouse` can easily sync hundreds of millions of rows of data (tens of GB) per hour.
* Supports multiple update strategies for immutable or mutable tables.
* Super easy to configure and run.

## Installation

    pip install pgwarehouse

Now you need to configure credentials for your **Postgres** source and the warehouse destination.

You can place Postgres credentials either in your config file or in your environment. If using the environment you need to set these variables:

    PGHOST
    PGDATABASE
    PGUSER
    PGPASSWORD
    PGSCHEMA (defaults to 'public')

## Creating a config file

Run this command to create a template config file:

    pgwarehouse init

This will create a local `pgwarehouse_conf.yaml` file. Now you can edit your Postgres credentials in the `postgres` stanza of the config file:

    postgres:
        pghost: (defaults to $PGHOST)
        pgdatabase: (defaults to $PGDATABASE
        pguser: (defaults to $PGUSER)
        pgpassword: (defaults to $PGPASSWORD)
        pgschema: (default to 'public')

## Specifying the warehouse credentials

Again you can use the environment or the config file. Set these sets of vars in your env:

    CLICKHOUSE_HOST
    CLICKHOUSE_DATABASE
    CLICKHOUSE_USER
    CLICKHOUSE_PWD

or

    SNOWSQL_ACCOUNT
    SNOWSQL_DATABASE
    SNOWSQL_SCHEMA
    SNOWSQL_WAREHOUSE
    SNOWSQL_USER
    SNOWSQL_PWD

or set these values in the `warehouse` stanza in the config file:

    warehouse:
        backend: (clickhouse|snowflake)
        clickhouse_host: 
        clickhouse_database: 
        clickhouse_user:
        clickhouse_password:
        --or--
        snowsql_account:
        snowsql_database:
        snowsql_schema:
        snowsql_warehouse:
        snowsql_user:
        snowsql_pwd:

The `snowsql_account` value should indicate your Snowflake account id. It's usually two parts separated by a '-'.

# Usage

Once the credentials are configured you can start syncing data. Start by listing tables from the Postgres database:

    pgwarehouse list

And you can see which tables exist so far in the warehouse:

    pgwarehouse listwh
    
Now use `sync` to sync a table (eg. the 'users' table):

    pgwarehouse sync users

Data will be downloaded from the Postgres database into CSV files on the local machine, and then those files will be uploaded to the warehouse. Using 'pgwarehouse listwh' should show the new table.

## Updating a table

After the initial sync has run, you can update the warehouse table with new records by running `sync` again:

    pgwarehouse sync users

## Syncing multiple tables

There are two ways to manage multiple tables. The first is just to use `all` as the table name:

    pgwarehouse sync all

This will attempt to sync ALL tables from Postgres into the warehouse. This could take a while!

The other way is to specify the `tables` list in the config file:

    tables:
        - users
        - charges
        - logs
