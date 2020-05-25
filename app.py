import dbtos3
import os
from dotenv import load_dotenv

# this loads environment variables
APP_ROOT = os.path.join(os.path.dirname(__file__))  # refers to application_top
load_dotenv(os.path.join(APP_ROOT, '.env'))

postgres = dbtos3.ReplicationMethodsPostgreSQL(
    host=os.getenv('POSTGRES_HOST'),
    database=os.getenv('POSTGRES_DATABASE'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_SECRET_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    s3bucket=os.getenv('S3_BUCKET'),
    main_key=os.getenv('POSTGRES_S3_MAIN_KEY')
)


def full_load_methods(args):
    for a in args:
        postgres.day_level_full_load(days=10, table=a, column='updated_at')


def replicate_methods(args):
    for a in args:
        postgres.replicate_table(table=a, column='updated_at')


if __name__ == '__main__':
    tables = ['users', 'orders']

    full_load_methods(tables)
    # replicate_methods(tables)
    postgres.close_connection()
