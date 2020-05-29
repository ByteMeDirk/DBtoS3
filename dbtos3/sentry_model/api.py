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

"""
For now, the sentry API only covers all projects, events and issues

get all project issues (totally unique)
for each issue get all events (these can potentially happen again)
for each event get all metadata

set fag to 1 for full load

projects = [...]
for project in projects:
    issues = api.list_events_by_project_and_issue_id(project.id, issue.id)
    for issue in issues:
        events = api.list_events_by_project_and_issues_id(project.id,  issue.id)
        for event in events:
            s3write(filename)
            
            
raw structure per project
- issues table
- events table

"""


class GetSentryEventsData:
    def __init__(self, **kwargs):
        """
        more can be found at https://docs.sentry.io/api/events/
        :param kwargs:
        auth_token=api bearer token for authorization
        organization=your company or organisation
        project=your specific project
        event_id=specific event id
        issue_id=specific issue id
        """
        self.header = {
            'Authorization': 'Bearer {}'.format(kwargs['auth_token'])
        }

    def list_a_projects_issues(self, organization, project, period):
        """
        Return a list of issues (groups) bound to a project.
        :param organization: string. organization name
        :param project: string. project name
        :param period: 0 = 24 hour, 1 = 14 days
        :return: json
        """

        # https://sentry.io/api/0/projects/howler/core-web/issues/?statsPeriod=24d
        url = 'https://sentry.io/api/0/projects/{0}/{1}/issues/?statsPeriod={2}'.format(
            organization, project, '14d' if period is 1 else '24h'
        )
        logging.info('[sentry.api] called : {}'.format(url))
        return requests.get(url, headers=self.header).json()

    def list_a_projects_events(self, organization, project, period):
        """
        Return a list of events bound to a project.
        :param organization: string. organization name
        :param project: string. project name
        :param period: 0 = 24 hour, 1 = 14 days
        :return: json
        """
        url = 'https://sentry.io/api/0/projects/{0}/{1}/events/?statsPeriod={2}'.format(
            organization, project, '14d' if period is 1 else '24h'
        )
        logging.info('[sentry.api] called : {}'.format(url))
        return requests.get(url, headers=self.header).json()

    def list_an_issues_events(self, issue_id):
        """
        This endpoint lists an issue’s events.
        :param issue_id: int. sentry unique id allocated to an issue
        :return: json
        """
        url = 'https://sentry.io/api/0/issues/{}/events/'.format(
            issue_id
        )
        logging.info('[sentry.api] called : {}'.format(url))
        return requests.get(url, headers=self.header).json()


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
    def update_catalogue(column_name, column_time, table_name, app_run_time, database):
        update_catalogue = catalogue.CatalogueMethods()
        update_catalogue.update_catalogue(column_name=column_name, column_time=column_time, table_name=table_name,
                                          app_run_time=app_run_time, data_source=database)

    def full_load(self, project):
        """
        full loads 14 days worth of sentry data to s3
        :param project: the sentry project name
        :return: json
        """
        try:
            logging.info('[sentry.api] attempting full load of sentry project: {}'.format(project))

            ###
            # ISSUES DATA
            # this is collected to iterate though all event issues and load them to s3
            # get all issues data for the project
            issues_data = self.sentry.list_a_projects_issues(project=project, organization=self.organization, period=1)
            issues_data_df = pd.json_normalize(issues_data)

            ###
            # EVENTS DATA
            # get all events data for the projects
            events_data = self.sentry.list_a_projects_events(project=project, organization=self.organization, period=1)
            events_data_df = pd.json_normalize(events_data)

            # write to catalog with max timestamp
            self.update_catalogue(column_name='dateCreated', column_time=events_data_df['dateCreated'].max(),
                                  table_name=project, app_run_time=datetime.now(), database='sentry-events-{}'
                                  .format(project))

            # write data to s3
            self.s3_service.write_to_s3(data=events_data, table=project + '-events')

            ###
            # EVENTS ISSUES DATA
            # first get all issue ids
            issue_ids = issues_data_df['id'].tolist()

            # gather all data per issue
            for i in issue_ids:
                events_issue_data = self.sentry.list_an_issues_events(issue_id=i)
                events_issue_data_df = pd.json_normalize(events_issue_data)

                # write to catalog with max timestamp
                self.update_catalogue(column_name='dateCreated', column_time=events_issue_data_df['dateCreated'].max(),
                                      table_name=project, app_run_time=datetime.now(),
                                      database='sentry-issue-{}'.format(i))

                # write data to s3
                self.s3_service.write_to_s3(data=events_data, table='{}-issue-{}'.format(project, i))

        except Exception as error:
            logging.info(
                '[sentry.api] error while doing a sentry full load: {} \n for project {}'.format(error, project))

    def replicate(self, project):
        """
        full loads 24 hours worth of sentry data to s3
        :param project:
        :return:
        """
        try:
            logging.info('[sentry.api] attempting replicate sentry project: {}'.format(project))

            ###
            # ISSUES DATA
            # this is collected to iterate though all event issues and load them to s3
            # get all issues data for the project
            issues_data = self.sentry.list_a_projects_issues(project=project, organization=self.organization, period=0)
            issues_data_df = pd.json_normalize(issues_data)

            ###
            # EVENTS DATA
            # get all events data for the projects
            events_data = self.sentry.list_a_projects_events(project=project, organization=self.organization, period=0)
            events_data_df = pd.json_normalize(events_data)

            # write to catalog with max timestamp
            self.update_catalogue(column_name='dateCreated', column_time=events_data_df['dateCreated'].max(),
                                  table_name=project, app_run_time=datetime.now(), database='sentry-events-{}'
                                  .format(project))

            # write data to s3
            self.s3_service.write_to_s3(data=events_data, table=project + '-events')

            ###
            # EVENTS ISSUES DATA
            # first get all issue ids
            issue_ids = issues_data_df['id'].tolist()

            # gather all data per issue
            for i in issue_ids:
                events_issue_data = self.sentry.list_an_issues_events(issue_id=i)
                events_issue_data_df = pd.json_normalize(events_issue_data)

                # write to catalog with max timestamp
                self.update_catalogue(column_name='dateCreated', column_time=events_issue_data_df['dateCreated'].max(),
                                      table_name=project, app_run_time=datetime.now(),
                                      database='sentry-issue-{}'.format(i))

                # write data to s3
                self.s3_service.write_to_s3(data=events_data, table='{}-issue-{}'.format(project, i))

        except Exception as error:
            logging.info(
                '[sentry.api] error while doing a sentry full load: {} \n for project {}'.format(error, project))
