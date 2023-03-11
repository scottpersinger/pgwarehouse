# pgwarehouse - quickly sync Postgres data to your cloud warehouse

## Introduction

Postgres is an amazing, general purpose OLTP database. But it's not designed for heavy analytic (OLAP) usage. Analytic queries are much better served by a columnar store database like Snowflake or Clickhouse.

This package allows you to easily sync data from a Postgres database into a local or cloud data warehouse (currently Snowflake or Clickhouse). You can perform a one-time sync operation, or run periodic incremental syncs to keep your warehouse up to date.

## Features

* High performance by using `COPY` to move lots of data efficiently. `pgwarehouse` can easily sync hundreds of millions of rows of data (tens of GB) per hour.
* Supports multiple update strategies for immutable or mutable tables.
* Easy to configure and run.

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
        pgschema: (defaults to 'public')

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

Data will be downloaded from the Postgres database into CSV files on the local machine, and then those files will be uploaded to the warehouse. Running `pgwarehouse listwh` will show the new table.

## Updating a table

After the initial sync has run, you can update the warehouse table with new records by running `sync` again:

    pgwarehouse sync users

See [update strategies](#table-update-strategies) for different ways to update your table on each sync.

## Syncing multiple tables

There are two ways to manage multiple tables. The first is just to pass `all` in place of the table name:

    pgwarehouse sync all

This will attempt to sync ALL tables from Postgres into the warehouse. This could take a while!

The other way is to specify the `tables` list in the config file:

    tables:
        - users
        - charges
        - logs

Now when you specify `sync all` the tool will use the list of tables specified in the config file.

**Pro tip!** You can add the `max_records` settings to your `postgres` configuration to limit the number
of records copied per table. This can be useful for testing the initial sync in case you have some
large tables. Set this value to something reasonable (like 10000) and then try syncrhonizing all
tables to make sure they copy properly. Once you have verified the tables in the warehouse then you
can remove this setting, drop any large tables, and then copy them in full (just run `sync all` again).

## Table update strategies

#### New Records Only (default)
The default update strategy is "new records only". This is done by selecting records with a greater value
for their primary id column than the greatest value currently in the warehouse. This strategy is simple
and quick, but only works for monotonically incrementing primary keys, and only finds new records.

#### Reload each time
Another supported strategy is "reload each time". This is the simplest strategy and we simply reload the
entire table every time we sync. This strategy should be fine for small-ish tables (like <10m rows).

#### Last Modified
Finally, if your table has a `last modified` column then you can use the "all modifications strategy".
In this case all records with a `last modified` timestamp greater than the maximum value found in the
warehouse will be selected and "upserted" into the warehouse. Records that are already present
(via matching the primary key) will be updated, and new records will be inserted.
    * The Snowflake backend uses the [MERGE](https://docs.snowflake.com/en/sql-reference/sql/merge) operation. 
    * The Clickhouse backend uses `ALTER TABLE .. DELETE` to remove matching records and then `INSERT` to insert the new values.

### What about deletes?

There is no simple way to capture deletes - you have to reload the entire table. A common pattern is
to apply new records on a daily basis, and reload the entire table every week to remove deleted records.

### What if my table has no primary key?

All the update strategies except "reload each time" require your table to have a primary key column.

## Specifying update strategy at the command line

    pgwarehouse sync <table>   (defaults to NEW RECORDS)
    pgwarehouse sync <table> last_modified=<last modified column>   (MODIFIED RECORDS)
    pgwarehouse reload <table> (reloads the whole table)

## Specifying update strategy in the config file

You can configure the update strategy selectively for each table in the config file. To do so,
specify the table as a nested dictionary with options:

    tables:
        - accounts
        - users:
            reload: true
        - orders:
            last_modified: updated_at
        - shoppers
            last_modified: update_time
            reload: sun
        - original_orders:
            skip: true

In this example:

* `accounts` will have new records only applied at each sync
* `users` will be reloaded completely on each sync
* `orders` will have modified records (found by the 'updated_at' column) applied on each sync
* `shoppers` will have modified records applied on each sync, except for any sync
which happens on Sunday, in which case the entire table will be reloaded.
* `original_orders` will be skipped entirely

The `reload` argument can take 3 forms:

    reload: true    - reload the table every sync
    reload: [sun,mon,tue,wed,thur,fri]  - reload if the sync occurs on this day of the week
    reload: 1-31    - reload if the sync occurs on this numeric day of the month (don't use 31!)

## Scheduling regular data syncs

`pgwarehouse` does not including any scheduling itself, you will need an external trigger like
`cron`, [Heroku Scheduler](https://devcenter.heroku.com/articles/scheduler), or a K8s
[CronJob](https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/).

When running, the tool will need access to local storage - potentially a lot if you are synchronizing
big tables. But nothing needs to persist between sync runs (except the config file) - the tool 
only relies on state it can query from Postgres or the warehouse.

## Troubleshooting

Sometimes when you are testing things out it can be helpful to do the sync in two phases:
1)download the data, 2)upload the data. You can use `extract` and `load` for this:

    pgwarehouse extract <table>     - only downloads data
    pgwarehouse load <table>        - loads the data into the warehouse

When the `extract` process runs, its stores data in `./pgw_data/<table name>_data`. As
files are uploaded they are moved into an `archive` subdirectory. When the **next sync**
runs then this archive directory will be cleaned up. This allows you to go examine
the CSV downloaded data in case the upload fails for some reason. 

## Limitations

Column type mapping today is [very limited](https://github.com/scottpersinger/pgwarehouse/blob/a20dc316bbdbc78317cfdd796216a847919d8755/pgwarehouse/snowflake_backend.py). More esoteric column types like JSON or ARRAY are simply
mapped as VARCHAR columns. Some of these types are supported in the warehouse and could be
implemented more accurately.

Composite primary keys (using multiple columns) have limited support. Today they will only work
with the RELOAD strategy.

Non-numeric primary key types (like UUIDs) probably won't work unless they have a good lexigraphic
sort that supports a `>` where clause.



