import calendar
import logging
import os
import time
from io import StringIO

import boto3
import pandas as pd

try:
    os.mkdir('Logs')
except FileExistsError:
    pass

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='Logs/s3.log', filemode='w', datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)


class S3ServiceMethod:
    """
    PostgreSQL_Model replication methods
    """

    def __init__(self, region_name, aws_access_key_id, aws_secret_access_key, s3bucket,
                 main_key):

        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        self.s3bucket = s3bucket
        self.s3main_key = main_key

        self.s3resource = boto3.resource('s3',
                                         region_name=region_name,
                                         aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key)

    def write_to_s3(self, table, data_frame):
        """
        gathers data frame object and parses it to s3 .csv object

        :param table: string. the table that is being replicated or loaded in order to name the directory accordingly
        :param data_frame: df object. the data frame object to be parsed into and s3 object
        :return: writes object directly to s3
        """
        try:
            logging.info('writing dataframe of table {} to s3'.format(table))
            if data_frame.empty:
                logging.info('no data in {} needs to be sent to s3'.format(table))
            else:
                csv_buf = StringIO()
                data_frame.to_csv(csv_buf)
                self.s3resource.Object(self.s3bucket,
                                       '{1}/{0}/{0}-{2}.csv'.format(table, self.s3main_key,
                                                                    calendar.timegm(time.gmtime()))) \
                    .put(Body=csv_buf.getvalue())

        except Exception as error:
            logging.info('Error while trying to send {} data to s3: {}'.format(table, error))

        except (Exception, pd.Error) as error:
            logging.info('Error while generating data with Pandas: {}'.format(error))

        finally:
            logging.info('loading data from {} to s3 done!'.format(table))
