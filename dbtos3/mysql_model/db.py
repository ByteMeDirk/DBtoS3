import logging
import os
from datetime import datetime

import mysql.connector
import pandas as pd

from dbtos3.s3_model import service
from dbtos3.sqlite_model import catalogue

try:
    os.mkdir('Logs')
except FileExistsError:
    pass

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='Logs/mysql.log', filemode='w', datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)


class ReplicationMethodsMySQL:
    """
    mySQL_Model replication methods
    """

    def __init__(self, host, database, user, password, region_name, aws_access_key_id, aws_secret_access_key, s3bucket,
                 main_key, port):
        """
        :param host: host name for db
        :param database: db name
        :param user: user name
        :param password: user password
        :param port: host port
        :param region_name: aws region
        :param aws_access_key_id: aws user key
        :param aws_secret_access_key: aws user password
        :param s3bucket: bucket to write to
        :param main_key: folder to write to
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port

        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        self.s3bucket = s3bucket
        self.s3main_key = main_key

        self.s3_service = service.S3ServiceMethod(
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            s3bucket=s3bucket,
            main_key=main_key
        )

        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            database=self.database,
            port=self.port
        )

        self.cursor = self.connection.cursor()

        # ensures the catalogue exists
        catalogue.CatalogueMethods().set_up_catalogue()

    def day_level_full_load(self, days, table, column):
        try:
            logging.info('loading data from {} at {} days based on column {}'.format(table, days, column))

            table_columns = []
            # construct query to get nth days of data from table & all column names of that table
            data_query = "select * from {} where {} > now() - interval {} day".format(table, column, days)
            column_query = "SHOW COLUMNS FROM {}".format(table)

            # execute queries and allocate them to objects
            self.cursor.execute(column_query)
            column_data = self.cursor.fetchall()

            for c in column_data:
                table_columns.append(c[0])

            self.cursor.execute(data_query)
            data_frame = pd.DataFrame(self.cursor.fetchall(), columns=table_columns)

            # updates catalogue
            self.update_catalogue(column_name=column, column_time=data_frame[column].max(),
                                  table_name=table, app_run_time=datetime.now(), database='mysql')

            # use write to s3 method to send data frame directly to s3
            self.s3_service.write_to_s3(data_frame=data_frame, table=table)

        except (Exception, pd.Error) as error:
            logging.info('Error while generating data with Pandas: {}'.format(error))

        except Exception as error:
            logging.info('Error while loading table from MySQL: {}'.format(error))

        finally:
            logging.info('loading data from {} at {} days based on column {} done!'.format(table, days, column))

    @staticmethod
    def update_catalogue(column_name, column_time, table_name, app_run_time, database):
        update_catalogue = catalogue.CatalogueMethods()
        update_catalogue.update_catalogue(column_name=column_name, column_time=column_time, table_name=table_name,
                                          app_run_time=app_run_time, database=database)

    def replicate_table(self, table, column):
        """
        gathers information from s3 .csv object and determines what data needs replication from the database
        :param table: string. the table that will be updated and replicated from
        :param column: string. the column that satisfies the date parameter for replication
        :return: writes directly to s3
        """
        try:
            table_columns = []

            logging.info('replicating table {} based on timestamp {}'.format(table, column))

            # get max update time first from catalogue
            max_update_time = catalogue.CatalogueMethods().get_max_time_from_catalogue(table=table, database='mysql')

            # construct query to get nth days of data from table & all column names of that table
            if len(max_update_time) == 0:
                logging.info('no need to update {}!'.format(table))
            else:
                data_query = "select * from {} where {} > '{}'".format(table, column, max_update_time)
                column_query = "show columns from {}".format(table)

                self.cursor.execute(column_query)
                column_data = self.cursor.fetchall()

                for c in column_data:
                    table_columns.append(c[0])

                # if method will pass the data if there is no updates needed
                self.cursor.execute(data_query)

                data_frame = pd.DataFrame(self.cursor.fetchall(), columns=table_columns)

                catalogue.CatalogueMethods().update_catalogue(column_name=column,
                                                              column_time=self.get_max_time_from_db(table=table,
                                                                                                    column=column),
                                                              table_name=table,
                                                              app_run_time=datetime.now(),
                                                              database='mysql')

                self.s3_service.write_to_s3(data_frame=data_frame, table=table)

        except (Exception, pd.Error) as error:
            logging.info('Error while generating data with Pandas: {}'.format(error))

        except Exception as error:
            logging.info('Error while loading table from MySQL: {}'.format(error))

        finally:
            logging.info('loading data from {} based on column {} done!'.format(table, column))

    def get_max_time_from_db(self, table, column):
        try:
            logging.info('getting max time from {} to update catalogue based on {}'.format(table, column))

            data_query = "select max({}) from {}".format(column, table)
            self.cursor.execute(data_query)
            return self.cursor.fetchall()[0][0]

        except (Exception, pd.Error) as error:
            logging.info('Error while generating data with Pandas: {}'.format(error))

        except Exception as error:
            logging.info('Error while loading table from MySQL: {}'.format(error))

        finally:
            logging.info('getting max time from {} complete! '.format(table))

    def close_connection(self):
        """
        closes connection to database
        :return: none
        """
        logging.info('Closing all connections')
        self.connection.close()
        catalogue.CatalogueMethods().close_connection()
