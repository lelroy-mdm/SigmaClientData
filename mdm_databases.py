import logging
log = logging.getLogger('mdmetl_databases')

import pyodbc
import yaml
import sys

try:
    with open (r'C:\\Users\\api-passwords.yaml', 'r') as y:
        SECRETS = yaml.safe_load(y)
except Exception as e:
    print(f'Error opening C:\\Users\\api-passwords.yaml')
    sys.exit(1)

def row_to_dict(row):
    return dict(zip([t[0] for t in row.cursor_description], row))


class MSSql:

    class MSSqlConnection:
        def __init__(self):
            # Establish the MSSQL connection
            ms_server = 'mdm-mssql.mdmbrookfield.com'
            ms_database = 'DevSCD'
            ms_username = SECRETS['MSSQL']['user']
            ms_password = SECRETS['MSSQL']['pass']
            ms_driver = '{ODBC Driver 17 for SQL Server}'

            self.con = pyodbc.connect(
                'DRIVER=' + ms_driver + ';SERVER=' + ms_server + ';DATABASE=' + ms_database + ';UID=' + ms_username +
                ';PWD=' + ms_password,
                autocommit=False)
            self.cur = self.con.cursor()
            self.cur.fast_executemany = True

        def __del__(self):
            self.con.close()


    def get_RawGiftFileColumnMap(self, client):

        db = self.MSSqlConnection()

        qry = 'select * from Config.RawGiftFileColumnMap where client=?'

        db.cur.execute(qry, client)

        rows = []
        for r in db.cur.fetchall():
            rows.append(row_to_dict(r))

        return rows

    def execute_simple_query(self, sql, values=None):

        db = self.MSSqlConnection()

        if values is None:
            db.cur.execute(sql)
        else:
            db.cur.execute(sql, values)

        db.cur.commit()


    def insert_rows(self, sql, data):

        db = self.MSSqlConnection()

        bulk_qty = 10

        # Break data into 10,000 row chunks
        total = len(data)
        if total == 0:
            return

        start_pos = 0
        while start_pos < total:
            last_pos = start_pos + bulk_qty if start_pos + bulk_qty<=total else total
            try:
                db.cur.executemany(sql, data[start_pos:last_pos])
                db.cur.commit()
            except Exception as e:
                log.error(e)
                for x in data[start_pos:last_pos]:
                    log.error(x)
            start_pos = last_pos
