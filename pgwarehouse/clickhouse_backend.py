import os
import shutil
import sys
import logging
import subprocess

from clickhouse_driver import Client as ClickhouseClient
import clickhouse_driver

from .backend import Backend, PGBackend

logger = logging.getLogger(__name__)

class ClickhouseBackend(Backend):
    ###############
    # Clickhouse
    ###############
    def __init__(self, config: dict, parent: PGBackend) -> None:
        self.clickhouse_host: str
        self.clickhouse_database: str
        self.clickhouse_user: str
        self.clickhouse_password: str
        self.config: dict = config
        self.parent: PGBackend = parent
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
        if pgtype == 'uuid':
            return "String"
        print(f"Warning, unknown postgres type: {pgtype} falling back to String")
        return "String"

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
            logger.debug(f"{cat} {input_file} | clickhouse-client --query {sql}")
            ret = subprocess.run(f'{cat} {input_file} | clickhouse-client -h {host} -d {database} -u {user} --password "{password}" --query "{sql}"', 
                                    shell=True, capture_output=True)            
        else:
            cmd = f'clickhouse-client {echo} -h {host} -d {database} -u {user} --password "{password}" --query "{sql}"'
            logger.debug(f"clickhouse-client --query {sql}")
            ret = subprocess.run(cmd, 
                                shell=True, capture_output=True)
        if ret.returncode != 0:
            logger.error(f"Command failed {ret}: {sql}")
            raise RuntimeError(f"Command failed {ret}: {sql}")
        return ret.stdout.decode('utf-8').strip()

    def count_table(self, table: str) -> int:
        rows = self.client.execute(f"SELECT count(*) FROM {table}")[0][0]
        return rows

    def load_table(self, table, schema_file, create_table=True, drop_table=False, csv_dir: str=None):
        if csv_dir is None:
            csv_dir = self.parent.csv_dir(table)
        if not os.path.exists(csv_dir):
            raise RuntimeError("Cannot find data dir: ", csv_dir)

        opts = self.parent.parse_schema_file(table, schema_file)
        if not opts['primary_key_cols']:
            print("Warning, no primary key found, will fallback to StripeLog table engine")

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
            if opts['primary_key_cols']:
                order_cols = ', '.join(opts['primary_key_cols'])
                engine_clause = f"ENGINE = MergeTree() ORDER BY ({order_cols})"
            else:
                engine_clause = "ENGINE = StripeLog"
            create_sql = f"""CREATE TABLE IF NOT EXISTS {table} ({create_structure}) {engine_clause};"""
            try:
                self.client.execute(create_sql)
            except clickhouse_driver.errors.ServerException as e:
                print(f"Error creating table {table}: \n{create_sql}\n{e}")
                raise

        logger.info("Sending to clickhouse...")
        
        archive_dir = csv_dir + "/archive"
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            logger.info(f"[clickhouse] INSERT INTO {table} from : {nextfile}")
            self.clickclient(f"""INSERT INTO {table} SELECT {select_clause} FROM input('{import_structure}') 
                FORMAT CSVWithNames SETTINGS date_time_input_format='best_effort';""", nextfile)
            shutil.move(nextfile, archive_dir)
            rows = self.client.execute(f"SELECT count(*) FROM {table}")[0][0]
            logger.info(f"{table} contains {rows} rows")

    def table_exists(self, table: str):
        rows = self.client.execute(f"SHOW TABLES LIKE '{table}'")
        return len(rows) > 0
    
    def _drop_table(self, table: str):
        return self.client.execute(f"DROP TABLE IF EXISTS {table}")

    def _query_table(self, table: str, cols: list[str], where: str, limit: int=None):
        colc = ", ".join(cols)
        wherec = f"WHERE {where}" if where is not None else ""
        limitc = f"LIMIT {limit}" if limit else ""
        sql = f"select {colc} from {table} {wherec} {limitc}"
        return self.client.execute(sql)

    def update_table(self, table: str, schema_file: str, upsert=False, last_modified: str = None, allow_create=False):
        if not self.table_exists(table):
            if allow_create:
                self.parent.extract(table, {})
                return self.load_table(table, schema_file, create_table=True)
            else:
                raise RuntimeError(f"Table {table} does not exist in Clickhouse")
            
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
            logger.info(f"Max {primary_key} value found in Clickhouse: {max_val}")
            if max_val is None:
                logger.warn("Warning, no value found for threshold column - loading all records")
            else:
                filter = f"where {primary_key} > {max_val} ORDER BY {primary_key}"
        else:    
            rows = self.client.execute(f'SELECT max({last_modified}) FROM {table}')
            for row in rows:
                max_val = row[0]
            logger.info(f"Max {last_modified} value found in Clickhouse: {max_val}")
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

    def merge_table(self, table:str, schema_file:str):
        # New+updated records have been downloaded to CSV already.
        # Clickhouse doesn't support Upsert, so instead we load the new 
        # records into a temp table, then we join the target table to the temp table
        # and delete existing records. Finally we Insert all the new records into the
        # target table and remove the temp table.

        opts = self.parent.parse_schema_file(table, schema_file)
        if not opts['primary_key_cols']:
            raise RuntimeError("No primary key for the table found, have to reload")

        temp_table = table + "__changes"
        csv_dir = self.parent.csv_dir(table)

        self.load_table(temp_table, schema_file, create_table=True, drop_table=True, csv_dir=csv_dir)
        primary_key = opts['primary_key_cols'][0]

        logger.debug(f"Deleting dupe records between {temp_table} and {table}")
        self.clickclient(f"""
            ALTER TABLE {table} DELETE WHERE {primary_key} IN (SELECT {primary_key} from {temp_table});
        """)
        logger.debug(f"Inserting new records into {table}")
        self.clickclient(f"""
            INSERT INTO {table} SELECT * FROM {temp_table};
        """)
        self.client.execute(f"DROP TABLE {temp_table}")





