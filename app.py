import os

import dbtos3
from dotenv import load_dotenv

APP_ROOT = os.path.join(os.path.dirname(__file__))  # refers to application_top
load_dotenv(os.path.join(APP_ROOT, '.env'))

core = dbtos3.ReplicationMethodsPostgreSQL(
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


def core_full_load_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        core.day_level_full_load(days=10, table=a, column='updated_at')


def core_replicate_methods(args):
    for a in args:
        print('--   new process {}  ----------------------------'.format(a))
        core.replicate_table(table=a, column='updated_at')


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
        sentry.full_load_all_project_issues(project=a)


if __name__ == '__main__':
    core_tables = ['tickets', 'ticket_types', 'ticket_categories', 'events', 'organisers', 'order_items',
                   'orders', 'payments', 'cashless_events', 'venues', 'users', 'donations', 'stickers',
                   'cashless_transactions']

    # core_full_load_methods(core_tables)
    core_replicate_methods(core_tables)
    core.close_connection()

    sentry_projects = ['cashless-app-bacardi', 'cashless-app-caipirinha', 'cashless-web', 'checkin-app', 'core-web',
                       'website-frontend']

    # sentry_full_load_methods(sentry_projects)
    sentry_replicate_methods(sentry_projects)
