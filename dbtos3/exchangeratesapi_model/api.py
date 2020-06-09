import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests

from dbtos3.s3_model import service
from dbtos3.sqlite_model import catalogue

try:
    os.mkdir('Logs')
except FileExistsError:
    pass

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='Logs/logs-{}.log'.format(datetime.now().strftime('%d%m%y%H%M')),
                    filemode='w', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)


def get_current_rates():
    """
    gets today's exchange rates at the time of the request
    :return: json
    """
    url = 'https://api.exchangeratesapi.io/{}/'.format(
        datetime.now().strftime('%Y-%m-%d')
    )
    logging.info('[exchangeratesapi.api] called : {} [{}]'.format(url, datetime.now()))
    return requests.get(url).json()


def get_ranged_rates(start_at):
    """
    gets range of exchange rates from start_at to current GMT day
    :param start_at: 'yyyy-mm-dd' date of start time
    :return: json
    """
    url = 'https://api.exchangeratesapi.io/history?start_at={}&end_at={}'.format(
        start_at, datetime.now().strftime('%Y-%m-%d')
    )
    logging.info('[exchangeratesapi.api] called : {} [{}]'.format(url, datetime.now()))
    return requests.get(url).json()


class ExchangesRatesReplicationMethod:
    """
    :param kwargs:
    no token needed
    organisation=your company or organisation
    """

    def __init__(self, **kwargs):
        self.s3_service = service.S3ServiceMethod(
            region_name=kwargs['region_name'],
            aws_access_key_id=kwargs['aws_access_key_id'],
            aws_secret_access_key=kwargs['aws_secret_access_key'],
            s3bucket=kwargs['s3bucket'],
            main_key=kwargs['main_key']
        )

    @staticmethod
    def update_catalogue(column_name, column_time, table_name, app_run_time, database):
        update_catalogue = catalogue.CatalogueMethods()
        update_catalogue.update_catalogue(column_name=column_name, column_time=column_time, table_name=table_name,
                                          app_run_time=app_run_time, data_source=database)

    def full_load(self, start_at):
        """
        full loads data from start date to current GMT date
        :param start_at: 'yyyy-mm-dd' format for the start date of replication
        :return: writes json data of global exchange rates
        """
        try:
            logging.info('[exchangerates.api] attempting full load of exchangeratesapi.io [{}]'.format(datetime.now()))
            data = get_ranged_rates(start_at=start_at)
            # at the moment this is the most elegant way I can think of to turn this data into a
            # useful table format
            rates = [v for k, v in data['rates'].items()][0]
            date = [k for k, v in data['rates'].items()]
            currency = [k for k, v in rates.items()]
            rate = [v for k, v in rates.items()]

            df = pd.DataFrame(list(zip(date, currency, rate)), columns=['date', 'currency', 'rate'])

            # write to catalog with max timestamp
            self.update_catalogue(column_name='date', column_time=df['date'].max(),
                                  table_name='exchangeratesmodel', app_run_time=datetime.now(),
                                  database='exchangeratesmodel.io')

            # write data to s3
            self.s3_service.write_to_s3(
                data=df.to_dict(orient='records'),
                local='exchangeratesapi'
            )

        except Exception as error:
            logging.info(
                '[exchangerates.api] error while doing a exchangeratesapi full load: {} [{}]'.format(error,
                                                                                                     datetime.now()))

    def replicate(self):
        """
        replicates the current exchange rate of the day
        :return: writes json data of global exchange rates
        """
        try:
            logging.info('[exchangerates.api] attempting replication of exchangeratesapi.io [{}]'.format(
                datetime.now()))
            data, df = get_current_rates(), pd.DataFrame(columns=['date', 'currency', 'rate'])

            for k, v in data['rates'].items():
                tmp = {
                    'date': data['date'], 'currency': k, 'rate': v
                }
                df = df.append(tmp, ignore_index=True)

            # test to see if replication is needed
            catalog_max_time = catalogue.CatalogueMethods() \
                .get_max_time_from_catalogue(table='exchangeratesmodel', data_source='exchangeratesmodel.io')
            df_max_time = df['date'].max()

            if catalog_max_time < df_max_time:
                # write to catalog with new max timestamp
                self.update_catalogue(column_name='date', column_time=df['date'].max(),
                                      table_name='exchangeratesmodel', app_run_time=datetime.now(),
                                      database='exchangeratesmodel.io')

                # write data to s3
                self.s3_service.write_to_s3(
                    data=df.to_dict(orient='records'),
                    local='exchangeratesapi'
                )
            else:
                logging.info('[exchangerates.api] no need to exchange rates! [{}]'.format(datetime.now()))

        except Exception as error:
            logging.info(
                '[exchangerates.api] error while doing a exchangeratesapi replication: {} [{}]'.format(error,
                                                                                                       datetime.now()))
