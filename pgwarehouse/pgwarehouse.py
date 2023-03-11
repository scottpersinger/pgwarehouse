#!/usr/bin/env python3
from collections import OrderedDict
import glob
import gzip
import logging
import os
import psycopg2
import re
import subprocess
import shutil
import shlex
from tabulate import tabulate
import traceback
import yaml
from datetime import datetime

from .backend import Backend, PGBackend
from .snowflake_backend import SnowflakeBackend
from .clickhouse_backend import ClickhouseBackend

logger = logging.getLogger('pgwarehouse')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(handler)


class PGWarehouse(PGBackend):
    def __init__(self, config_file: str = "", command: str = "", table: str="", data_dir: str=".",
                backend_type: str= "", last_modified: str=None, debug=False) -> None:

        self.backend_type: str
        self.backend: Backend
        self.config: dict
        self.pghost: str
        self.pgdatabase: str
        self.pguser: str
        self.pgpassword: str

        if debug:
            logger.setLevel(logging.DEBUG)
            handler.setLevel(logging.DEBUG)
        self.data_dir: str = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.config: dict = {}
        warehouse_config = {}
        if command == 'init':
            return self.init(config_file)
        
        if config_file:
            self.config = yaml.safe_load(open(config_file))
            if 'warehouse' in self.config:
                self.backend_type: str = self.config['warehouse'].get('backend', backend_type)
                warehouse_config = self.config['warehouse']
        else:
            self.backend_type = backend_type
        if not self.backend_type:
            raise RuntimeError("Must specify the warehouse backend")
        self.setup_pg_env()

        if self.backend_type == 'clickhouse':
            self.backend = ClickhouseBackend(warehouse_config, self)
        elif self.backend_type == 'snowflake':
            self.backend = SnowflakeBackend(warehouse_config, self)
        else:
            raise RuntimeError(f"Unknown backend: {self.backend_type}")

        def get_table_opts(tablename, last_modified=None):
            for t in self.config.get('tables', []):
                if t == tablename:
                    return {}
                elif isinstance(t, dict) and tablename in t:
                    return t[tablename]
            return {'last_modified':last_modified} if last_modified else {}

        def get_all_tables():
            if 'tables' in self.config:
                return self.config['tables']
            else:
                return self.all_table_names()
            
        tables:list = []
        if table == 'all':
            print(f"============== {command} ALL TABLES =========== ")
            for table in get_all_tables():
                table_opts = get_table_opts(table)
                self.table = table
                self.schema_file = os.path.join(self.data_dir, table + ".schema")
                logger.info(f">>>>>>>>> {command} {table}")
                try:
                    getattr(self, command)(self.table, table_opts)
                except RuntimeError:
                    print(f"ERROR: {command} {table}")
                    traceback.print_exc()
                logger.info("<<<<<<<<<\n")
            print("============== DONE =========== ")
        elif table:
            if not self.table_exists(table):
                print(f"Error, table {table} not found in Postgres")
                return
            table_opts = get_table_opts(table, last_modified=last_modified)
            self.table = table
            self.schema_file = os.path.join(self.data_dir, table + ".schema")
            logger.info(f"=== {command} {table}")
            getattr(self, command)(self.table, table_opts)
            logger.info("")
        elif command in ['list', 'listwh']:
            getattr(self, command)()
        else:
            print("Bad arguments")

    ###############
    # Commands
    ###############

    def init(self, config_file: str):
        if os.path.exists(config_file):
            print("Config file already exists")
        else:
            print("This will create a pgwarehouse config file in the current directory.")
            backend = None
            while backend not in ["1", "2"]:
                backend = input("Choose your warehouse type (Snowflake - 1, Clickhouse - 2): ")
            backend = ["snowflake", "clickhouse"][int(backend)-1]
            conf = {
                "postgres": {
                    'pghost':"",
                    'pgdatabase':"",
                    'pguser':"",
                    'pgpassword':"",
                    'pgschema': 'public'
                },
                "warehouse": {
                    "backend": backend
                }
            }
            if backend == "snowflake":
                for key in ['snowsql_account',  'snowsql_warehouse', 'snowsql_database', 'snowsql_user', 'snowsql_password']:
                    conf['warehouse'][key] = ""
            elif backend == "clickhouse":
                for key in ['clickhouse_host', 'clickhouse_database', 'clickhouse_user', 'clickhouse_password']:
                    conf['warehouse'][key] = ""
            with open(config_file, "w") as f:
                f.write(yaml.dump(conf))
                f.write("# Add a 'tables' list to specify tables to sync\n")
            print(f"Wrote config file {config_file}.\nPlease edit the file to set your Postgres and Warehouse connection details.")

    def iterate_csv_files(self, csv_dir):
        files = glob.glob(os.path.join(csv_dir, "*.gz"))
        for idx, file in enumerate(sorted(files, key=lambda x: int(re.findall(r"\d+", "0,"+x)[-1]))):
            yield idx, file

    def csv_dir(self, table: str):
        return os.path.join(self.data_dir, table + "_data")

    def get_log_handler(self) -> logging.Handler:
        return handler
    
    def listwh(self):
        self.backend.list_tables()

    def load(self, table: str, table_opts: dict, drop_table=False):
        # balk if table exists
        self.backend.load_table(table, self.schema_file, drop_table=drop_table)

    def sync(self, table: str, table_opts: dict):
        if table_opts.get('reload') == True:
            return self.reload(table, table_opts)
        
        self.backend.update_table(
            table, 
            self.schema_file, 
            upsert='last_modified' in table_opts, 
            allow_create=True,
            last_modified=table_opts.get('last_modified'))

    def reload(self, table: str, table_opts: dict):
        logger.debug(f"RELOADING table {table}")
        self.extract(table, table_opts)
        self.load(table, table_opts, drop_table=True)

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
        self.pgport = int(conf.get('pgport', os.environ.get('PGPORT', '5432')))
        self.max_pg_records = conf.get('max_records', None)
        self.client = psycopg2.connect(
            f"host={self.pghost} dbname={self.pgdatabase} user={self.pguser} password={self.pgpassword} port={self.pgport}",
            connect_timeout=10
        )
        self.cursor: psycopg2.extensions.cursor = self.client.cursor()

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
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=['schema', 'table', 'size', 'rows']))

    def count_warehouse_table(self, table):
        return self.backend.count_table(table)

    def all_table_names(self):
        self.cursor.execute(
            f"select table_name from information_schema.tables where table_schema='{self.pgschema}'"
        )
        return sorted([r[0] for r in self.cursor.fetchall()])

    def dump_schema(self, table: str, schema_file: str):
        ret = os.system(f"psql --pset=format=unaligned -c \"\\d {table}\" > {schema_file}")
        if ret != 0:
            raise RuntimeError("Error saving schema")
        logger.debug(f"Saved schema to {schema_file}")

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
            logger.info(f"Writing to {fname} total records written: {total_records:,} total bytes: {current_size:,}")
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
            if current_size > max_size or (self.max_pg_records and total_records >= self.max_pg_records):
                outfile.close()
                file_suffix += 1
                outfile = next_file()
                outfile.write(header)
                current_size = 0
            if self.max_pg_records and total_records >= self.max_pg_records:
                logger.warn("Max records reached")
                break
        outfile.close()
        proc.stdout.close()
        proc.wait()

        if proc.returncode != 0:
            logger.error(f"Error extracting table {table}")
        logger.debug("Wrote csv files")
        logger.info(f"Done: total records written: {total_records:,} total bytes: {current_size:,}")
        return [file_suffix, total_records]

    def parse_schema_file(self, table, schema_file) -> dict:
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
                        primary_key_cols = [col.strip().strip('"') for col in m.group(1).split(",")]

        return {'columns': columns, 'primary_key_cols': primary_key_cols}

    def table_exists(self, table: str):
        sql = f"select 1 from information_schema.tables where table_schema='{self.pgschema}' and table_name='{table}'"
        self.cursor.execute(sql)
        return self.cursor.fetchone() is not None
    
