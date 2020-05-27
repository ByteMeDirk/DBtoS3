import os

from dotenv import load_dotenv

import dbtos3

APP_ROOT = os.path.join(os.path.dirname(__file__))  # refers to application_top
load_dotenv(os.path.join(APP_ROOT, '.env'))

###
# Setting up PostgreSQL Replication and full-load
###

website_db = dbtos3.ReplicationMethodsPostgreSQL(
    host=os.getenv('POSTGRES_HOST'),
    database=os.getenv('POSTGRES_DATABASE'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_SECRET_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    s3bucket=os.getenv('S3_BUCKET'),
    main_key=os.getenv('POSTGRES_S3_MAIN_KEY'),
    port=os.getenv('POSTGRES_PORT')
)


def website_db_full_load_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        website_db.day_level_full_load(days=10, table=a, column='updated_at')


def website_db_replicate_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        website_db.replicate_table(table=a, column='updated_at')


###
# Setting up MySQL Replication and full-load
###

mysql_db = dbtos3.ReplicationMethodsMySQL(
    host=os.getenv('MYSQL_HOST'),
    database=os.getenv('MYSQL_DATABASE'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_SECRET_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    s3bucket=os.getenv('S3_BUCKET'),
    main_key=os.getenv('MYSQL_S3_MAIN_KEY'),
    port=os.getenv('MYSQL_PORT')
)


def mysql_db_full_load_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        mysql_db.day_level_full_load(days=10, table=a, column='updated_at')


def mysql_db_replicate_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        mysql_db.replicate_table(table=a, column='updated_at')


###
# Setting up Sentry Replication and full-load
###
sentry = dbtos3.SentryReplicationMethod(
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_SECRET_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    s3bucket=os.getenv('S3_BUCKET'),
    main_key=os.getenv('SENTRY_S3_MAIN_KEY'),
    auth_token=os.getenv('SENTRY_AUTH_TOKEN'),
    organization=os.getenv('SENTRY_ORGANIZATION'),
)


def sentry_full_load_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        sentry.full_load_all_events(project=a)
        sentry.full_load_all_project_issues(project=a)


def sentry_replicate_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        sentry.replicate_all_events(project=a)
        sentry.replicate_all_project_issues(project=a)


if __name__ == '__main__':
    website_db_tables = ['users']
    website_db_full_load_methods(website_db_tables)
    website_db_replicate_methods(website_db_tables)
    website_db.close_connection()

    mysql_tables = ['tasks']
    mysql_db_full_load_methods(mysql_tables)
    mysql_db_replicate_methods(mysql_tables)
    mysql_db.close_connection()

    sentry_projects = ['website-frontend']
    sentry_full_load_methods(sentry_projects)
    sentry_replicate_methods(sentry_projects)
