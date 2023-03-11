from abc import ABC, abstractmethod
import logging
from typing import Iterator, Tuple, Iterable

class Backend(ABC):
    @abstractmethod
    def list_tables(self):
        pass

    @abstractmethod
    def load_table(self, table: str, schema_file: str, drop_table=False):
        pass

    @abstractmethod
    def update_table(self, table: str, schema_file: str, upsert=False, last_modified: str = None, allow_create=False):
        pass

    @abstractmethod
    def count_table(self, table: str) -> int:
        pass

    @abstractmethod
    def _drop_table(self, table: str):
        pass

    @abstractmethod
    def _query_table(self, table: str, cols: list[str], where: str, limit: int=None) -> Iterable:
        pass


class PGBackend(ABC):
    @abstractmethod
    def dump_schema(self, table: str, schema_file: str):
        pass

    @abstractmethod
    def extract(self, table: str, table_opts: dict, filter="") -> tuple[int,int]:
        pass

    @abstractmethod
    def parse_schema_file(self, table, schema_file) -> dict:
        pass

    @abstractmethod
    def csv_dir(self, table: str) -> str:
        pass

    @abstractmethod
    def get_log_handler(self) -> logging.Handler:
        pass

    @abstractmethod
    def iterate_csv_files(self, csv_dir) -> Iterator[Tuple[int, str]]:
        pass

