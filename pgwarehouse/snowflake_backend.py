from datetime import date
import logging
import os
import shutil
import subprocess

import snowflake.connector

from .backend import Backend, PGBackend

logger = logging.getLogger('pgwarehouse')

def return_output(cmd):
    val = subprocess.run(cmd, shell=True, capture_output=True, check=True)
    return val.stdout.decode('utf-8').strip()

class SnowflakeBackend(Backend):
    ###############
    # Snowflake
    ###############
    RESERVED_COL_NAMES = ['current_date','order','to','from']

    def __init__(self, config: dict, parent: PGBackend) -> None:
        self.snowsql_account: str
        self.snowsql_database: str
        self.snowsql_schema: str
        self.snowsql_warehouse: str
        self.snowsql_user: str
        self.snowsql_pwd: str
        self.config = config
        self.parent: PGBackend = parent
        self.setup_env()
        
    def setup_env(self):
        for key in ['snowsql_account', 'snowsql_database', 'snowsql_schema','snowsql_warehouse','snowsql_user','snowsql_pwd']:
            val = self.config.get(key, os.environ.get(key.upper()))
            if val is None:
                raise RuntimeError(f"Missing {key} in config file or environment")
            setattr(self, key, val)

        # Gets the version
        ctx = snowflake.connector.connect(
            user=self.snowsql_user,
            password=self.snowsql_pwd,
            account=self.snowsql_account,
            database=self.snowsql_database
            )
        self.snow_cursor = ctx.cursor()
        self.snow_cursor.execute("use warehouse " + self.snowsql_warehouse + "; ")

        sflogger = logging.getLogger('snowflake.connector')
        sflogger.setLevel(logging.WARNING)
        sflogger.addHandler(self.parent.get_log_handler())

    def table_exists(self, table: str):
        self.snow_cursor.execute(f"select count(*) from information_schema.tables where table_name ilike '{table}'")
        return self.snow_cursor.fetchone()[0] >= 1

    def count_table(self, table: str) -> int:
        return self.snow_cursor.execute("select count(*) from {self.snowsql_schema}.{table}").fetchone()[0]

    def _drop_table(self, table: str):
        return self.snow_cursor.execute("DROP TABLE IF EXISTS {self.snowsql_schema}.{table}")

    def pg_to_sf_root_type(self, pgtype: str):
        if pgtype.endswith("_enum"):
            return "STRING"
        if pgtype.startswith("boolean"):
            return "BOOLEAN"
        if pgtype.startswith("character") or pgtype.startswith("jsonb") or pgtype == "text":
            return "STRING"
        if pgtype.startswith("time "):
            return "TIME"
        if pgtype.startswith("date"):
            return "DATETIME"
        if pgtype.startswith("timestamp"):
            return "TIMESTAMP"
        if pgtype.startswith("int") or pgtype.startswith("bigint"):
            return "BIGINT"
        if pgtype.startswith("smallint"):
            return "SMALLINT"
        if pgtype.startswith("numeric") or pgtype.startswith("real") or pgtype.startswith("double"):
            return "NUMERIC"
        if pgtype == 'year':
            return "String"
        logger.warn(f"Warning unknown Postgres type {pgtype}, falling back to String")
        return "String"

    def pg_to_sf_type(self, pgtype: str):
        if pgtype.endswith("[]"):
            return "String" 
            # figure out how to parse CSV arrays. Need to sub '[' for '{' and then use JSONExtract(col,'Array(Int)')
            # "Array(" + ch_root_type(pgtype) + ")"
            # 
        else:
            return self.pg_to_sf_root_type(pgtype)

    def quote_col(self, colname: str):
        if colname.lower() in SnowflakeBackend.RESERVED_COL_NAMES:
            return f'"{colname}"'
        else:
            return colname

    def list_tables(self):
        for row in self.snow_cursor.execute("SHOW TABLES;"):
            print(row)                              

    def load_table(self, table, schema_file, create_table=True, drop_table=False):
        csv_dir = self.parent.csv_dir(table)
        if not os.path.exists(csv_dir):
            raise RuntimeError("Cannot find data dir: ", csv_dir)

        opts = self.parent.parse_schema_file(table, schema_file)
        import_structure = ", ".join([f"{self.quote_col(col)} {self.pg_to_sf_type(ctype)}" for col, ctype in opts['columns'].items()])

        if drop_table:
            self.snow_cursor.execute(f"DROP TABLE IF EXISTS {self.snowsql_database}.{self.snowsql_schema}.{table}")

        if create_table:
            self.snow_cursor.execute(f"USE DATABASE {self.snowsql_database}")
            self.snow_cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.snowsql_schema}.{table} ({import_structure});")

        archive_dir = csv_dir + "/archive"
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        logger.info(f"Sending to {table} data to Snowflake...")

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            logger.debug(f"Loading file: {nextfile}")
            skip = 1
            if not nextfile.endswith(".gz"):
                print(return_output(f"gzip -9 {nextfile}"))
                nextfile += ".gz"
            csv = os.path.basename(nextfile)
            self.snow_cursor.execute(f"USE SCHEMA {self.snowsql_schema}")
            logger.debug("PUTing file")
            self.snow_cursor.execute(f"PUT file://{nextfile} @{self.snowsql_database}.{self.snowsql_schema}.%{table};")
            logger.info(f"COPY INTO INTO {self.snowsql_database}.{self.snowsql_schema}.{table} FROM @%{table} PATTERN = '{csv}'")
            for row in self.snow_cursor.execute(f""" 
                COPY INTO {self.snowsql_database}.{self.snowsql_schema}.{table} FROM @%{table}
                    FILE_FORMAT = (type = csv field_optionally_enclosed_by='\\"' SKIP_HEADER={skip}) ON_ERROR=CONTINUE FORCE=TRUE 
                    PATTERN = '{csv}'
                PURGE = TRUE
            """):
                logger.info(row)
            shutil.move(nextfile, archive_dir)

    def merge_table(self, table:str, schema_file:str, allow_create=False):
        if not self.table_exists(table):
            if allow_create:
                return self.load_table(table, schema_file, create_table=True)
            else:
                raise RuntimeError(f"Table {table} does not exist in Snowflake")
        csv_dir = self.parent.csv_dir(table)
        if not os.path.exists(csv_dir):
            raise RuntimeError("Cannot find data dir: ", csv_dir)

        opts = self.parent.parse_schema_file(table, schema_file)
        columns = opts['columns'].keys()
        column_list = ", ".join([self.quote_col(c) for c in columns])
        key_col = opts['primary_key_cols'][0]

        archive_dir = csv_dir + "/archive"
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        logger.info(f"Sending {table} updates to Snowflake...")

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            logger.debug(f"Loading file: {nextfile}")
            if not nextfile.endswith(".gz"):
                print(return_output(f"gzip -9 {nextfile}"))
                nextfile += ".gz"
            csv = os.path.basename(nextfile)
            self.snow_cursor.execute(f"USE SCHEMA {self.snowsql_schema}")
            logger.debug("PUTing file")
            self.snow_cursor.execute(f"PUT file://{nextfile} @{self.snowsql_database}.{self.snowsql_schema}.%{table};")
            logger.debug("LOADing file into database")
            update_sets = ", ".join([f'{table}.{self.quote_col(col)} = csvsrc.{self.quote_col(col)}' for col in columns])
            values_list = ", ".join([f'csvsrc.{self.quote_col(col)}' for col in columns])
            self.snow_cursor.execute(f"""
                CREATE OR REPLACE FILE FORMAT pgw_csv_format TYPE = 'csv' SKIP_HEADER = 1 
                FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE
            """)
            logger.info(f"MERGE INTO INTO {self.snowsql_database}.{self.snowsql_schema}.{table} PATTERN = '{csv}'")
            self.snow_cursor.execute(f""" 
                MERGE INTO {self.snowsql_database}.{self.snowsql_schema}.{table} USING 
                    (SELECT 
                    {column_list} 
                    FROM @%{table}(FILE_FORMAT => 'pgw_csv_format', PATTERN => '{csv}')
                    ) csvsrc
                    ON csvsrc.{key_col} = {table}.{key_col}
                    WHEN MATCHED THEN UPDATE SET {update_sets}
                    WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({values_list})
            """)
            logger.debug(f"Removing staged file '{csv}'")
            self.snow_cursor.execute(f"REMOVE @%{table} PATTERN = '{csv}'")
            shutil.move(nextfile, archive_dir)

    def update_table(self, table: str, schema_file: str, upsert=False, last_modified: str = None, allow_create=False):
        if not self.table_exists(table):
            if allow_create:
                self.parent.extract(table, {})
                return self.load_table(table, schema_file, create_table=True)
            else:
                raise RuntimeError(f"Table {table} does not exist in Clickhouse")

        if not os.path.exists(schema_file):
            self.parent.dump_schema(table, schema_file)
        if upsert and last_modified is None:
            print("*** WARNING: Upsert requested but no 'last_modified' columnn specified. Falling back to UPDATE")
            upsert = False

        opts = self.parent.parse_schema_file(table, schema_file)
        if not opts['primary_key_cols']:
            raise RuntimeError("No primary key for the table found, have to reload")
        if len(opts['primary_key_cols']) > 1:
            raise RuntimeError("Not sure how to incremental update with multiple primary key cols")
        primary_key = opts['primary_key_cols'][0]

        out_dir = os.path.join(table + "_data", str(date.today()))

        max_val = None
        filter = ""
        if not upsert:
            rows = self.snow_cursor.execute(f'SELECT max({primary_key}) FROM {table}').fetchall()
            for row in rows:
                max_val = row[0]
            logger.info(f"Max {primary_key} value found in Snowflake: {max_val}")
            if max_val is None:
                logger.warn("Warning, no value found for threshold column - loading all records")
            else:
                filter = f"where {primary_key} > {max_val} ORDER BY {primary_key}"
        else:    
            rows = self.snow_cursor.execute(f'SELECT max({last_modified}) FROM {table}').fetchall()
            for row in rows:
                max_val = row[0]
            logger.info(f"Max {last_modified} value found in Snowflake: {max_val}")
            if max_val is None:
                logger.warn("Warning, no value found for threshold column - loading all records")
                upsert = False
            else:
                filter = f"where {last_modified} >= '{max_val}' ORDER BY {last_modified}"

        # Now extract PG records where the primary key is greater than what's in Clickhouse
        [file_count, record_count] = self.parent.extract(
            table, 
            {}, 
            filter=filter
        )
        if record_count == 0:
            logger.info("Warehouse table is up to date")
            return

        if upsert:
            # perform a real upsert in case some of the "new" records may be updates
            self.merge_table(table, schema_file)
        else:
            self.load_table(table, schema_file, create_table=False)


