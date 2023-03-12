import argparse

from dotenv import load_dotenv

from .pgwarehouse import PGWarehouse

load_dotenv()

parser = argparse.ArgumentParser(description='Postgres to Warehouse sync utility.', prog = 'pgwarehouse')
parser.add_argument(
    'command', 
    help="Operation: list (pg tables), listwh (warehouse tables), sync (sync table to warehouse), reload (reload a table)",
    choices=['init','list','listwh','extract','load','sync','reload']
)
parser.add_argument('table', help="Table name or 'all' to use the config", nargs='?')
parser.add_argument('--config', help="Config file", default='./pgwarehouse_conf.yaml', required=False)
parser.add_argument('--data', help="Temp storage directory", default='./pgw_data', required=False)
parser.add_argument('--backend', help="Warehouse type", choices=['clickhouse', 'snowflake'], required=False)
parser.add_argument('--last-modified', help="Timestamp column to use to select records for Upsert", required=False)
parser.add_argument('-d', '--debug', action='store_true', required=False)

args = parser.parse_args()

def main():
    pgw = PGWarehouse(
        config_file=args.config, 
        command=args.command, 
        table=args.table,
        data_dir=args.data, 
        backend_type=args.backend, 
        last_modified=args.last_modified,
        debug=args.debug)

if __name__ == '__main__':
    main()


