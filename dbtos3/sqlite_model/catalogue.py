from datetime import datetime
import logging
import os
import sqlite3

try:
    os.mkdir('Logs')
except FileExistsError:
    pass

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='Logs/logs-{}.log'.format(datetime.now().strftime('%d%m%y%H%M')),
                    filemode='w', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)


class CatalogueMethods:
    def __init__(self):
        self.conn = sqlite3.connect('catalogue.db')
        self.cursor = self.conn.cursor()

    def set_up_catalogue(self):
        """
        sets up the catalogue before any other models begin their tasks
        :return: none
        """
        if self.conn is not None:
            try:
                logging.info('[sqlite.catalogue] setting up data catalogue')
                catalogue_query = """
                    CREATE TABLE IF NOT EXISTS catalogue (
                        id integer PRIMARY KEY AUTOINCREMENT,
                        column_name text NOT NULL,
                        column_time text NOT NULL ,
                        table_name text NOT NULL,
                        app_run_time text NOT NULL,
                        data_source text NOT NULL 
                    )"""

                self.cursor.execute(catalogue_query)
                logging.info('[sqlite.catalogue] catalogue created successfully')

            except (Exception, sqlite3.Error) as error:
                logging.info('[sqlite.catalogue] error while creating catalogue: {}'.format(error))

        else:
            logging.info('[sqlite.catalogue] cannot connect to catalogue')

    def update_catalogue(self, column_name, column_time, table_name, app_run_time, data_source):
        """
        updates the catalogue whenever a full load or replication is done
        :param column_name: string. the name of the column that satisfies the replication time
        :param column_time: string. the time of the column that satisfies the replication time
        :param table_name: string. the name of the table that will be replicated or loaded
        :param app_run_time: string. the time the application ran
        :param data_source: the name of the database model that was used, this allows for multiple data sources in one app
        :return: none
        """
        if self.conn is not None:
            try:
                catalogue_query = "INSERT INTO catalogue (column_name, column_time, table_name, app_run_time, data_source) " \
                                  "VALUES('{}','{}','{}','{}','{}')".format(
                    column_name, column_time, table_name, app_run_time, data_source)

                self.cursor.execute(catalogue_query)
                self.conn.commit()
                logging.info('[sqlite.catalogue] catalogue updated successfully')

            except (Exception, sqlite3.Error) as error:
                logging.info('[sqlite.catalogue] error while creating catalogue: {}'.format(error))

        else:
            logging.info('[sqlite.catalogue] cannot connect to catalogue')

    def get_max_time_from_catalogue(self, table, data_source):
        """
        gathers the max time of the relevant table from the catalogue
        :param table: string. the table that needs to be satisfied with a timestamp
        :param data_source: the name of the database model that was used, this allows for multiple data sources in one app
        :return: timestamp
        """
        if self.conn is not None:
            try:
                catalogue_query = "SELECT max(column_time) FROM catalogue WHERE table_name = '{}' and data_source = '{}'"\
                    .format(table, data_source)

                self.cursor.execute(catalogue_query)
                logging.info('[sqlite.catalogue] max gathered from catalogue successfully')
                return self.cursor.fetchall()[0][0]

            except (Exception, sqlite3.Error) as error:
                logging.info('[sqlite.catalogue] error while gathering max from catalogue: {}'.format(error))

        else:
            logging.info('[sqlite.catalogue] cannot connect to catalogue')

    def close_connection(self):
        """
        closes connection to database
        :return: none
        """
        self.conn.close()
