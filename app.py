import os

from dotenv import load_dotenv

import dbtos3

# this loads environment variables
APP_ROOT = os.path.join(os.path.dirname(__file__))  # refers to application_top
load_dotenv(os.path.join(APP_ROOT, '.env'))

sentry = dbtos3.SentryReplicationMethod(
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_SECRET_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    s3bucket=os.getenv('S3_BUCKET'),
    main_key=os.getenv('SENTRY_S3_MAIN_KEY'),
    auth_token=os.getenv('SENTRY_AUTH_TOKEN'),
    organization=os.getenv('SENTRY_ORGANIZATION'),
)


def full_load_methods(args):
    for a in args:
        sentry.full_load_all_events(project=a)


def replicate_methods(args):
    for a in args:
        sentry.replicate_all_events(project=a)


if __name__ == '__main__':
    # full_load_methods(['core-web'])
    replicate_methods(['core-web'])
