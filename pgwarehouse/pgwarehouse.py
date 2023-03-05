#!/usr/bin/env python3
from collections import OrderedDict
import glob
import gzip
from clickhouse_driver import Client as ClickhouseClient
import logging
import os
import platform
import sys
import re
import subprocess
import shutil
import shlex
import traceback
import yaml
from typing import Union
from datetime import datetime, date
import snowflake.connector

def return_output(cmd):
    val = subprocess.run(cmd, shell=True, capture_output=True, check=True)
    return val.stdout.decode('utf-8').strip()


class PGWarehouse:
    def __init__(self, config_file: str = "", command: str = "", table: str="", data_dir: str=".",
                backend: str= "") -> None:
        self.data_dir: str = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.config: dict = {}
        warehouse_config = {}
        if config_file:
            self.config = yaml.safe_load(open(config_file))
            if 'warehouse' in self.config:
                self.backend_type: str = self.config['warehouse'].get('backend', backend)
                warehouse_config = self.config['warehouse']
        if not self.backend_type:
            raise RuntimeError("Must specify the warehouse backend")
        self.setup_pg_env()

        self.snowflake = self.clickhouse = False
        if self.backend_type == 'clickhouse':
            self.backend = ClickhouseBackend(warehouse_config, self)
        elif self.backend_type == 'snowflake':
            self.backend = SnowflakeBackend(warehouse_config, self)

        def get_table_opts(tablename):
            if tablename in self.config['tables'] and isinstance(self.config['tables'].get(tablename), dict):
                return self.config['tables'].get(tablename)
            else:
                return {}

        tables:list = []
        if table:
            self.table_opts = {table: get_table_opts(table)}
            self.table = table
            self.schema_file = os.path.join(self.data_dir, table + ".schema")
            getattr(self, command)(self.table, self.table_opts)
        elif table == 'all':
            print(f"============== {command} ALL TABLES =========== ")
            for table in self.config['tables']:
                self.table_opts = {table: get_table_opts(table)}
                self.table = table
                self.schema_file = os.path.join(self.data_dir, table, ".schema")
                getattr(self, command)(self.table, self.table_opts)
            print("============== DONE =========== ")
        elif command in ['list', 'listwh']:
            getattr(self, command)()
        else:
            print("Bad arguments")

    ###############
    # Postgres
    ###############

    def setup_pg_env(self):
        conf = self.config.get('postgres', {})
        for key in ['pghost','pgdatabase','pguser','pgpassword']:
            val = conf.get(key, os.environ.get(key.upper()))
            if val is None:
                raise RuntimeError(f"Missing {key} in config file or environment")
            setattr(self, key, val)
            os.environ[key.upper()] = val
        self.pgschema = conf.get('pgschema', os.environ.get('PGSCHEMA', 'public'))

    def list(self) -> None:
        sql = """
            SELECT table_schema, table_name, pg_size_pretty(total_bytes) AS total, to_char(row_estimate, 'FM999,999,999,999') as rows
            FROM (
            SELECT *, total_bytes-index_bytes-coalesce(toast_bytes,0) AS table_bytes FROM (
                SELECT c.oid,nspname AS table_schema, relname AS table_name
                        , c.reltuples AS row_estimate
                        , pg_total_relation_size(c.oid) AS total_bytes
                        , pg_indexes_size(c.oid) AS index_bytes
                        , pg_total_relation_size(reltoastrelid) AS toast_bytes
                    FROM pg_class c
                    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE relkind = 'r' and nspname='public'
            ) a order by table_bytes desc
            ) a;
        """
        sql = sql.strip().replace("\n", " ")
        print(return_output(f"psql -c \"{sql}\""))

    def dump_schema(self, table: str, schema_file: str):
        ret = os.system(f"psql --pset=format=unaligned -c \"\\d {table}\" > {schema_file}")
        if ret != 0:
            raise RuntimeError("Error saving schema")
        print(f"Saved schema to {schema_file}")

    def extract(self, table: str, table_opts: dict, filter=""):
        # Returns a tuple of [file count, line count] of downloaded records
        self.dump_schema(table, self.schema_file)
        print(datetime.now(), f" Extracting table with COPY {table} {filter} to csv...")

        header = None
        current_size = 0
        max_size = (1024**3)*1 # 2GB
        file_suffix = 1
        total_records = 0

        out_dir = os.path.join(self.data_dir, table + "_data")
        shutil.rmtree(out_dir, ignore_errors=True)
        os.makedirs(out_dir, exist_ok=True)

        def next_file():
            fname = os.path.join(out_dir, f"{table}{file_suffix}0.csv.gz")
            print(f"Writing to {fname} total records written: {total_records:,} total bytes: {current_size:,}")
            return gzip.open(fname, "wt")

        outfile = next_file()
        cmd = f'psql -c "\\copy (select * from {table} {filter}) to STDOUT CSV HEADER\"'
        args = shlex.split(cmd)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        for line in iter(proc.stdout.readline, b''):
            strline = line.decode('utf-8')
            if header is None:
                header = strline
            else:
                total_records += 1
            outfile.write(strline) # [:-1] to cut off newline char
            current_size += len(strline)
            if current_size > max_size:
                outfile.close()
                file_suffix += 1
                outfile = next_file()
                outfile.write(header)
                current_size = 0
        outfile.close()
        proc.stdout.close()
        proc.wait()

        if proc.returncode != 0:
            print("Error extracting table")
            sys.exit(1)
        print(datetime.now(), f" Wrote csv files")
        print(f"Done: total records written: {total_records:,} total bytes: {current_size:,}")
        return [file_suffix, total_records]

    def parse_schema_file(self, table, schema_file):
        line: str = ""
        inside_cols: bool = False
        inside_idxs: bool = False
        columns: OrderedDict = OrderedDict()
        primary_key_cols = []

        with open(schema_file) as f:
            for line in f.readlines():
                m = re.search(r"able \"(\w+)\.(\w+)", line)
                if m:
                    schema = m.group(1)
                    sc_table = m.group(2)
                    if sc_table != table:
                        print("Error, schema references the wrong table: ", sc_table)
                if line.startswith("Column|"):
                    inside_cols = True
                    continue
                if line.startswith("Indexes:"):
                    inside_cols = False
                    inside_idxs = True
                    continue
                if inside_cols and line.count("|") >= 4:
                    cname, ctype,collation,nullable,cdefault = line.split("|")
                    columns[cname] = ctype
                if inside_idxs:
                    m = re.search(r"PRIMARY KEY.*\((.*)\)", line)
                    if m:
                        primary_key_cols = m.group(1).split(",")
                        print("Primary cols: ", primary_key_cols)

        return {'columns': columns, 'primary_key_cols': primary_key_cols}

    ###############
    # Warehouse
    ###############

    def iterate_csv_files(self, csv_dir):
        files = glob.glob(os.path.join(csv_dir, "*.gz"))
        for idx, file in enumerate(sorted(files, key=lambda x: int(re.findall(r"\d+", "0,"+x)[-1]))):
            yield idx, file

    def csv_dir(self, table: str):
        return os.path.join(self.data_dir, table + "_data")

    def listwh(self):
        self.backend.list_tables()

    def load(self, table: str, table_opts: dict, drop_table=False):
        # balk if table exists
        self.backend.load_table(table, self.schema_file, drop_table=drop_table)

    def sync(self, table: str, table_opts: dict):
        self.extract(table, table_opts)
        self.load(table, table_opts)

    def resync(self, table: str, table_opts: dict):
        self.extract(table, table_opts)
        self.load(table, table_opts, drop_table=True)

    def update(self, table: str, table_opts: dict):
        self.backend.update_table(table, self.schema_file, upsert=False)

    # elif command == 'upsert_snowflake':
    #     verify_snow_env()
    #     last_modified = None
    #     for v in sys.argv:
    #         if v.startswith("last_modified="):
    #             last_modified = v[len("last_modified="):]
    #     if last_modified is None:
    #         print("Must supply last_modified=<column> argument")
    #         sys.exit(1)
    #     update_table_snowflake(get_table_arg(), get_schema_file(), get_warehouse_arg(), 
    #                             last_modified=last_modified, upsert=True)
    



class ClickhouseBackend:
    ###############
    # Clickhouse
    ###############
    def __init__(self, config: dict, parent: PGWarehouse) -> None:
        self.clickhouse_host: str
        self.clickhouse_database: str
        self.clickhouse_user: str
        self.clickhouse_password: str
        self.config: dict = config
        self.parent: PGWarehouse = parent
        self.setup_clickhouse_env()

    def setup_clickhouse_env(self):
        for key in ['clickhouse_host', 'clickhouse_user', 'clickhouse_password']:
            val = self.config.get(key, os.environ.get(key.upper()))
            if val is None:
                raise RuntimeError(f"Missing {key} in config file or environment")
            setattr(self, key, val)
        self.clickhouse_database = self.config.get('clickhouse_database', os.environ.get('CLICKHOUSE_DATABASE', 'default'))
        if shutil.which("clickhouse-client") is None:
            raise RuntimeError("clickhouse-client not found in PATH")
        self.client: ClickhouseClient = ClickhouseClient(
            host=self.clickhouse_host,
            user=self.clickhouse_user,
            password=self.clickhouse_password,
            database=self.clickhouse_database
        )

    def list_tables(self):
        print(self.clickclient("SHOW TABLES;", echo=True))

    def convert_pg_root_type_clickhouse(self, pgtype: str, for_parse=False):
        if pgtype.endswith("_enum"):
            return "String"
        if pgtype.startswith("boolean"):
            return "Bool"
        if pgtype.startswith("character") or pgtype.startswith("jsonb") or pgtype == "text":
            return "String"
        if pgtype.startswith("time "):
            return "String"
        if pgtype.startswith("date"):
            if for_parse:
                return "String"
            return "DateTime"
        if pgtype.startswith("timestamp"):
            if for_parse:
                return "String"
            return "DateTime64(3)"
        if pgtype.startswith("int") or pgtype.startswith("bigint"):
            return "Int64"
        if pgtype.startswith("smallint"):
            return "Int32"
        if pgtype.startswith("numeric") or pgtype.startswith("real") or pgtype.startswith("double"):
            return "Float64"
        if pgtype == 'year':
            return "String"
        raise RuntimeError("Unknown postgres type: " + pgtype)

    def convert_pg_type_clickhouse(self, pgtype: str, for_parse=False):
        if pgtype.endswith("[]"):
            return "String" 
            # figure out how to parse CSV arrays. Need to sub '[' for '{' and then use JSONExtract(col,'Array(Int)')
            # "Array(" + ch_root_type(pgtype) + ")"
            # 
        else:
            return self.convert_pg_root_type_clickhouse(pgtype, for_parse=for_parse)

    def clickclient(self, sql, input_file = None, echo=False):
        host = self.clickhouse_host
        user = self.clickhouse_user
        password = self.clickhouse_password
        database = self.clickhouse_database

        if echo:
            echo = "--echo"
        else:
            echo = ""

        if input_file:
            cat = "gzcat" if input_file.endswith(".gz") else "cat"
            print(f"{cat} {input_file} | clickhouse-client --query {sql}")
            ret = subprocess.run(f'{cat} {input_file} | clickhouse-client -h {host} -d {database} -u {user} --password "{password}" --query "{sql}"', 
                                    shell=True, capture_output=True)            
        else:
            cmd = f'clickhouse-client {echo} -h {host} -d {database} -u {user} --password "{password}" --query "{sql}"'
            print(f"clickhouse-client --query {sql}")
            ret = subprocess.run(cmd, 
                                shell=True, capture_output=True)
        if ret.returncode != 0:
            print(f"Command failed {ret}: {sql}")
            sys.exit(1)
        return ret.stdout.decode('utf-8').strip()

    def load_table(self, table, schema_file, create_table=True, drop_table=False):
        csv_dir = self.parent.csv_dir(table)
        if not os.path.exists(csv_dir):
            raise RuntimeError("Cannot find data dir: ", csv_dir)

        opts = self.parent.parse_schema_file(table, schema_file)
        if not opts['primary_key_cols']:
            raise RuntimeError("Cannot create table with no primary key found")

        import_structure = ", ".join(
            [f"{col} {self.convert_pg_type_clickhouse(ctype, for_parse=True)}" for col, ctype in opts['columns'].items()]
        )
        select_cols = [
            f"parseDateTimeBestEffortOrNull({col})" if (ctype.startswith("date") or ctype.startswith("time")) else f"{col}" 
            for col, ctype in opts['columns'].items()
        ]
        select_clause = ", ".join(select_cols)

        if drop_table:
            self.client.execute(f"DROP TABLE IF EXISTS {table}")

        if create_table:
            cols: list =[(col, self.convert_pg_type_clickhouse(ctype)) for col, ctype in opts['columns'].items()]
            cols = [
                (col, f"Nullable({ctype})") if col not in opts['primary_key_cols'] else (col, ctype) for col, ctype in cols
            ]
            create_structure = ", ".join([f"{c[0]} {c[1]}" for c in cols])
            order_cols = ', '.join(opts['primary_key_cols'])
            self.client.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} ({create_structure}) ENGINE = MergeTree() ORDER BY ({order_cols});
            """)

        print("Sending to clickhouse...")
        
        archive_dir = csv_dir + "/archive"
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            print(f"Loading file: {nextfile} into clickhouse")
            self.clickclient(f"""INSERT INTO {table} SELECT {select_clause} FROM input('{import_structure}') 
                FORMAT CSVWithNames SETTINGS date_time_input_format='best_effort';""", nextfile)
            print("Moving file to archive")
            shutil.move(nextfile, archive_dir)
            rows = self.client.execute(f"SELECT count(*) FROM {table}")[0][0]
            print(f"{table} contains {rows} rows")

    def merge_table(self, table:str, schema_file:str):
        pass

    def update_table(self, table: str, schema_file, upsert=False):
        if not os.path.exists(schema_file):
            self.parent.dump_schema(table, schema_file)

        opts = self.parent.parse_schema_file(table, schema_file)
        if not opts['primary_key_cols']:
            raise RuntimeError("No primary key for the table found, have to reload")
        if len(opts['primary_key_cols']) > 1:
            raise RuntimeError("Not sure how to incremental update with multiple primary key cols")
        primary_key = opts['primary_key_cols'][0]

        max_val = None
        filter = ""
        if not upsert:
            rows = self.client.execute(f'SELECT max({primary_key}) FROM {table}')
            for row in rows:
                max_val = row[0]
            print(f"Max {primary_key} value found in Clickhouse: ", max_val)
            if max_val is None:
                print("Warning, no value found for threshold column - loading all records")
            else:
                filter = f"where {primary_key} > {max_val} ORDER BY {primary_key}"
        else:    
            rows = self.client.execute(f'SELECT max({last_modified}) FROM {table}')
            for row in rows:
                max_val = row[0]
            print(f"Max {last_modified} value found in Clickhouse: ", max_val)
            if max_val is None:
                print("Warning, no value found for threshold column - loading all records")
                upsert = False
            else:
                filter = f"where {last_modified} >= '{max_val}' ORDER BY {last_modified}"

        # Now extract PG records where the primary key is greater than what's in Clickhouse
        [file_count, record_count] = self.parent.extract(
            table, 
            schema_file, 
            filter=filter
        )
        if record_count == 0:
            print("Warehouse table is up to date")
            return

        if upsert:
            # perform a real upsert in case some of the "new" records may be updates
            self.merge_table(table, schema_file)
        else:
            self.load_table(table, schema_file, create_table=False)

class SnowflakeBackend:
    ###############
    # Snowflake
    ###############
    RESERVED_COL_NAMES = ['current_date','order','to','from']

    def __init__(self, config: dict, parent: PGWarehouse) -> None:
        self.snowsql_account: str
        self.snowsql_database: str
        self.snowsql_schema: str
        self.snowsql_warehouse: str
        self.snowsql_user: str
        self.snowsql_pwd: str
        self.config = config
        self.parent: PGWarehouse = parent
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

        logger = logging.getLogger('snowflake.connector')
        logger.setLevel(logging.WARNING)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(funcName)s() - %(message)s'))
        logger.addHandler(ch)

    def table_exists_snowflake(self, table: str):
        self.snow_cursor.execute(f"select count(*) from information_schema.tables where table_name ilike '{table}'")
        return self.snow_cursor.fetchone()[0] >= 1

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
        print(f"Warning unknown Postgres type {pgtype}, falling back to String")
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
        database = os.environ['SNOWSQL_DATABASE']

        opts = self.parent.parse_schema_file(table, schema_file)
        import_structure = ", ".join([f"{self.quote_col(col)} {self.pg_to_sf_type(ctype)}" for col, ctype in opts['columns'].items()])

        if drop_table:
            self.snow_cursor.execute(f"DROP TABLE IF EXISTS {table}")

        if create_table:
            self.snow_cursor.execute(f"USE DATABASE {database}")
            self.snow_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({import_structure});")

        archive_dir = csv_dir + "/archive"
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        print("Sending to Snowflake...")

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            print(f"Loading file: {nextfile}")
            skip = 1
            if not nextfile.endswith(".gz"):
                print(return_output(f"gzip -9 {nextfile}"))
                nextfile += ".gz"
            csv = os.path.basename(nextfile)
            self.snow_cursor.execute("USE SCHEMA public")
            print("PUTing file")
            self.snow_cursor.execute(f"PUT file://{nextfile} @{database}.public.%{table};")
            print("LOADing file into database")
            for row in self.snow_cursor.execute(f""" 
                COPY INTO public.{table} FROM @%{table}
                    FILE_FORMAT = (type = csv field_optionally_enclosed_by='\\"' SKIP_HEADER={skip}) ON_ERROR=CONTINUE FORCE=TRUE 
                    PATTERN = '{csv}'
                PURGE = TRUE
            """):
                print(row)
            print("Moving file to archive")
            shutil.move(nextfile, archive_dir)

    def merge_table(self, table:str, schema_file:str):
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

        print("Sending merges to Snowflake...")

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            print(f"Loading file: {nextfile}")
            if not nextfile.endswith(".gz"):
                print(return_output(f"gzip -9 {nextfile}"))
                nextfile += ".gz"
            csv = os.path.basename(nextfile)
            self.snow_cursor.execute("USE SCHEMA public")
            print("PUTing file")
            self.snow_cursor.execute(f"PUT file://{nextfile} @{self.snowsql_database}.public.%{table};")
            print("LOADing file into database")
            update_sets = ", ".join([f'{table}.{self.quote_col(col)} = csvsrc.{self.quote_col(col)}' for col in columns])
            values_list = ", ".join([f'csvsrc.{self.quote_col(col)}' for col in columns])
            self.snow_cursor.execute(f"""
                CREATE OR REPLACE FILE FORMAT pp_csv_format TYPE = 'csv' SKIP_HEADER = 1 
                FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE
            """)
            self.snow_cursor.execute(f""" 
                MERGE INTO public.{table} USING 
                    (SELECT 
                    {column_list} 
                    FROM @%{table}(FILE_FORMAT => 'pp_csv_format', PATTERN => '{csv}')
                    ) csvsrc
                    ON csvsrc.{key_col} = {table}.{key_col}
                    WHEN MATCHED THEN UPDATE SET {update_sets}
                    WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({values_list})
            """)
            self.snow_cursor.execute(f"REMOVE @%{table} PATTERN = '{csv}'")
            print("Moving file to archive")
            shutil.move(nextfile, archive_dir)

    def update_table(self, table: str, schema_file, upsert=False, last_modified: str = None):
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
            print(f"Max {primary_key} value found in Snowflake: ", max_val)
            if max_val is None:
                print("Warning, no value found for threshold column - loading all records")
            else:
                filter = f"where {primary_key} > {max_val} ORDER BY {primary_key}"
        else:    
            rows = self.snow_cursor.execute(f'SELECT max({last_modified}) FROM {table}').fetchall()
            for row in rows:
                max_val = row[0]
            print(f"Max {last_modified} value found in Snowflake: ", max_val)
            if max_val is None:
                print("Warning, no value found for threshold column - loading all records")
                upsert = False
            else:
                filter = f"where {last_modified} >= '{max_val}' ORDER BY {last_modified}"

        # Now extract PG records where the primary key is greater than what's in Clickhouse
        [file_count, record_count] = self.parent.extract(
            table, 
            schema_file, 
            filter=filter
        )
        if record_count == 0:
            print("Warehouse table is up to date")
            return

        if upsert:
            # perform a real upsert in case some of the "new" records may be updates
            self.merge_table(table, schema_file)
        else:
            self.load_table(table, schema_file, create_table=False)



