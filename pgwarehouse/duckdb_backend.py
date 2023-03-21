import logging
import os
import shutil

import duckdb

from .backend import Backend, PGBackend

logger = logging.getLogger(__name__)

# Write a class which implements the Backend interface using DuckDB as the
# underlying database.



class DuckdbBackend(Backend):   
    ###############
    # DuckDB
    ###############
    def __init__(self, config: dict, parent: PGBackend) -> None:
        self.duckdb_path: str
        self.config: dict = config
        self.parent: PGBackend = parent
        self.setup_duckdb_env()

    def setup_duckdb_env(self):
        self.duckdb_path = self.config.get('duckdb_path', './duck.db')
        self.duck = duckdb.connect(self.duckdb_path)

    def list_tables(self):
        print("\n".join(list([r[0] for r in self.duck.execute("SHOW TABLES;").fetchall()])))

    def convert_pg_root_type_duck(self, pgtype: str, for_parse=False):
        if pgtype.endswith("_enum"):
            return "String"
        if pgtype.startswith("boolean"):
            return "BOOLEAN"
        if pgtype.startswith("character") or pgtype.startswith("jsonb") or pgtype == "text":
            return "VARCHAR"
        if pgtype.startswith("time "):
            return "TIMESTAMP"
        if pgtype.startswith("date"):
            if for_parse:
                return "String"
            return "TIMESTAMP"
        if pgtype.startswith("timestamp"):
            if for_parse:
                return "String"
            return "TIMESTAMP"
        if pgtype.startswith("int") or pgtype.startswith("bigint"):
            return "BIGINT"
        if pgtype.startswith("smallint"):
            return "INTEGER"
        if pgtype.startswith("numeric") or pgtype.startswith("real") or pgtype.startswith("double"):
            return "DOUBLE"
        if pgtype == 'year':
            return "VARCHAR"
        if pgtype == 'uuid':
            return "UUID"
        print(f"Warning, unknown postgres type: {pgtype} falling back to VARCHAR")
        return "VARCHAR"

    def convert_pg_type_duck(self, pgtype: str, for_parse=False):
        if pgtype.endswith("[]"):
            return "VARCHAR" 
        else:
            return self.convert_pg_root_type_duck(pgtype, for_parse=for_parse)

    def count_table(self, table: str) -> int:
        return self.duck.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]

    def _quote_column(self, col_name, ctype: str, keys: list= []):
        res = col_name.replace(" ","_") + " " + ctype
        if col_name in keys:
            res += " PRIMARY KEY"
        return res
    
    def load_table(self, table, schema_file, create_table=True, drop_table=False, csv_dir: str=None):
        if csv_dir is None:
            csv_dir = self.parent.csv_dir(table)
        if not os.path.exists(csv_dir):
            raise RuntimeError("Cannot find data dir: ", csv_dir)

        opts = self.parent.parse_schema_file(table, schema_file)

        if drop_table:
            self.duck.execute(f"DROP TABLE IF EXISTS {table}")

        if create_table:
            cols: list =[(col, self.convert_pg_type_duck(ctype)) for col, ctype in opts['columns'].items()]
            create_structure = ", ".join([self._quote_column(c[0], c[1], keys=opts['primary_key_cols']) for c in cols])
            create_sql = f"""CREATE TABLE IF NOT EXISTS {table} ({create_structure});"""
            try:
                self.duck.execute(create_sql)
            except Exception as e:
                print(f"Error creating table {table}: \n{create_sql}\n{e}")
                raise

        logger.info("Sending to duckdb...")
        
        archive_dir = csv_dir + "/archive"
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            logger.info(f"[duckdb] INSERT INTO {table} from : {nextfile}")
            self.duck.execute(f"COPY {table} FROM '{nextfile}' (HEADER)") 
            shutil.move(nextfile, archive_dir)
            rows = self.duck.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            logger.info(f"{table} contains {rows} rows")

    def table_exists(self, table: str):
        try:
            self.duck.execute(f"SELECT 1 from '{table}'")
            return True
        except duckdb.CatalogException:
            return False
    
    def _drop_table(self, table: str):
        return self.duck.execute(f"DROP TABLE IF EXISTS {table}")

    def _query_table(self, table: str, cols: list[str], where: str, limit: int=None):
        colc = ", ".join(cols)
        wherec = f"WHERE {where}" if where is not None else ""
        limitc = f"LIMIT {limit}" if limit else ""
        sql = f"select {colc} from {table} {wherec} {limitc}"
        return self.duck.execute(sql).fetchall()

    def update_table(self, table: str, schema_file: str, upsert=False, last_modified: str = None, allow_create=False):
        if not self.table_exists(table):
            if allow_create:
                self.parent.extract(table, {})
                return self.load_table(table, schema_file, create_table=True)
            else:
                raise RuntimeError(f"Table {table} does not exist in duckdb")
            
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
            rows = self.duck.execute(f'SELECT max({primary_key}) FROM {table}').fetchall()
            for row in rows:
                max_val = row[0]
            logger.info(f"Max {primary_key} value found in duckdb: {max_val}")
            if max_val is None:
                logger.warn("Warning, no value found for threshold column - loading all records")
            else:
                filter = f"where {primary_key} > {max_val} ORDER BY {primary_key}"
        else:    
            rows = self.duck.execute(f'SELECT max({last_modified}) FROM {table}').fetchall()
            for row in rows:
                max_val = row[0]
            logger.info(f"Max {last_modified} value found in duckdb: {max_val}")
            if max_val is None:
                logger.warn("Warning, no value found for threshold column - loading all records")
                upsert = False
            else:
                filter = f"where {last_modified} >= '{max_val}' ORDER BY {last_modified}"

        # Now extract PG records where the primary key is greater than what's in duckdb
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
        csv_dir = self.parent.csv_dir(table)
        archive_dir = csv_dir + "/archive"
        os.makedirs(archive_dir, exist_ok=True)

        for idx, nextfile in self.parent.iterate_csv_files(csv_dir):
            logger.info(f"[duckdb] INSERT OR REPLACE INTO {table} from : {nextfile}")
            sql = f"INSERT OR REPLACE INTO {table} SELECT * from read_csv_auto('{nextfile}', HEADER=TRUE);"
            logger.info(sql)
            self.duck.execute(sql)
            shutil.move(nextfile, archive_dir)
            rows = self.duck.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            logger.info(f"{table} contains {rows} rows")


   
