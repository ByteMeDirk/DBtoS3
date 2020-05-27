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

logging.basicConfig(filename='Logs/logs.log', filemode='w', datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)

"""
For now, the sentry API only covers all projects, events and issues
"""


class GetSentryEventsData:
    def __init__(self, **kwargs):
        """
        more can be found at https://docs.sentry.io/api/events/
        :param kwargs:
        auth_token=api bearer token for authorization
        organisation=your company or organisation
        project=your specific project
        event_id=specific event id
        issue_id=specific issue id
        """
        self.header = {
            'Authorization': 'Bearer {}'.format(kwargs['auth_token'])
        }

    def list_project_events(self, organization, project):
        """
        Return a list of events bound to a project.
        :return: json
        """
        url = 'https://sentry.io/api/0/projects/{0}/{1}/events/'.format(
            organization, project
        )

        return requests.get(url, headers=self.header).json()

    def retrieve_event_for_project(self, organization, project, event_id):
        """
        Return details on an individual event.
        :return: json
        """
        url = 'https://sentry.io/api/0/projects/{0}/{1}/events/{2}/'.format(
            organization, project, event_id
        )
        return requests.get(url, headers=self.header).json()

    def list_project_issues(self, organization, project):
        """
        Return a list of issues (groups) bound to a project.
        :return: json
        """
        url = 'https://sentry.io/api/0/projects/{0}/{1}/issues/'.format(
            organization, project
        )
        return requests.get(url, headers=self.header).json()

    def retrieve_an_issue(self, organization, project):
        """
        Return details on an individual issue.
        :return: json
        """
        url = 'https://sentry.io/api/0/projects/{0}/{1}/issues/'.format(
            organization, project
        )
        return requests.get(url, headers=self.header).json()

    def list_issue_events(self, issue_id):
        """
        This endpoint lists an issue’s events.
        :return: json
        """
        url = 'https://sentry.io/api/0/issues/{0}/events/'.format(
            issue_id
        )
        data_frame = self.json_to_df(requests.get(url, headers=self.header).json())
        return data_frame


class SentryReplicationMethod:
    def __init__(self, **kwargs):
        """
        :param kwargs:
        auth_token=api bearer token for authorization
        organisation=your company or organisation
        """
        self.organization = kwargs['organization']
        self.auth_token = kwargs['auth_token']

        self.sentry = GetSentryEventsData(auth_token=self.auth_token, organization=self.organization)

        self.s3_service = service.S3ServiceMethod(
            region_name=kwargs['region_name'],
            aws_access_key_id=kwargs['aws_access_key_id'],
            aws_secret_access_key=kwargs['aws_secret_access_key'],
            s3bucket=kwargs['s3bucket'],
            main_key=kwargs['main_key']
        )

    @staticmethod
    def json_to_df(data):
        return pd.json_normalize(data)

    @staticmethod
    def update_catalogue(column_name, column_time, table_name, app_run_time, database):
        update_catalogue = catalogue.CatalogueMethods()
        update_catalogue.update_catalogue(column_name=column_name, column_time=column_time, table_name=table_name,
                                          app_run_time=app_run_time, data_source=database)

    def full_load_all_events(self, project):
        """
        full loads all event data for a project
        :param project: the name of your sentry project
        :return: writes directly to s3
        """
        try:
            logging.info('Attempting full load of sentry project: {}'.format(project))
            # get all data
            data = self.sentry.list_project_events(project=project, organization=self.organization)
            data_frame = self.json_to_df(data)

            self.update_catalogue(column_name='dateCreated', column_time=data_frame['dateCreated'].max(),
                                  table_name=project, app_run_time=datetime.now(), database='sentry-events')

            # use write to s3 method to send data frame directly to s3
            self.s3_service.write_to_s3(data=data, table=project + '-events')

        except Exception as error:
            logging.info('Error while doing a sentry full load: {}'.format(error))

    def replicate_all_events(self, project):
        """
        replicates all event data for a project
        :param project: the name of your sentry project
        :return: writes directly to s3
        """
        try:
            logging.info('Attempting replication of sentry project: {}'.format(project))
            # get all data
            data = self.sentry.list_project_events(project=project, organization=self.organization)
            data_frame = self.json_to_df(data)

            # get max time for replication
            max_time = catalogue.CatalogueMethods().get_max_time_from_catalogue(table=project,
                                                                                data_source='sentry-events')

            # select all relevant data from data frame
            data_frame = data_frame[data_frame['dateCreated'] >= max_time]

            if max_time is None:
                logging.info('no need to update {}!'.format(project))
            else:
                # updates catalogue
                self.update_catalogue(column_name='dateCreated', column_time=data_frame['dateCreated'].max(),
                                      table_name=project, app_run_time=datetime.now(), database='sentry-events')

                # use write to s3 method to send data frame directly to s3
                self.s3_service.write_to_s3(data=data, table=project + '-events')

        except Exception as error:
            logging.info('Error while doing a sentry full load: {}'.format(error))

    def full_load_all_project_issues(self, project):
        """
        full loads all project issues
        :param project: the name of your sentry project
        :return: writes directly to s3
        """
        try:
            logging.info('Attempting full load of sentry project issues: {}'.format(project))
            # get all data
            data = self.sentry.list_project_issues(project=project, organization=self.organization)
            data_frame = self.json_to_df(data)

            # updates catalogue
            self.update_catalogue(column_name='lastSeen', column_time=data_frame['lastSeen'].max(),
                                  table_name=project, app_run_time=datetime.now(),
                                  database='sentry-project-issues')

            # use write to s3 method to send data frame directly to s3
            self.s3_service.write_to_s3(data=data, table=project + '-project-issues')

        except Exception as error:
            logging.info('Error while doing a sentry full load: {}'.format(error))

    def replicate_all_project_issues(self, project):
        """
        replicates all project issues
        :param project: the name of your sentry project
        :return: writes directly to s3
        """
        try:
            logging.info('Attempting replication of sentry project issues: {}'.format(project))
            # get all data
            data = self.sentry.list_project_issues(project=project, organization=self.organization)
            data_frame = self.json_to_df(
                self.sentry.list_project_issues(project=project, organization=self.organization))

            # get max time for replication
            max_time = catalogue.CatalogueMethods().get_max_time_from_catalogue(table=project,
                                                                                data_source='sentry-project-issues')

            # select all relevant data from data frame
            data_frame = data_frame[data_frame['lastSeen'] >= max_time]

            if max_time is None:
                logging.info('no need to update {}!'.format(project))
            else:
                # updates catalogue
                self.update_catalogue(column_name='lastSeen', column_time=data_frame['lastSeen'].max(),
                                      table_name=project, app_run_time=datetime.now(), database='sentry-events')

                # use write to s3 method to send data frame directly to s3
                self.s3_service.write_to_s3(data=data, table=project + '-project-issues')

        except Exception as error:
            logging.info('Error while doing a sentry full load: {}'.format(error))
