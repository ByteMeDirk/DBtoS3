"""
  _____  ____  _        _____ ____
 |  __ \|  _ \| |      / ____|___ \
 | |  | | |_) | |_ ___| (___   __) |
 | |  | |  _ <| __/ _ \\___ \ |__ <
 | |__| | |_) | || (_) |___) |___) |
 |_____/|____/ \__\___/_____/|____/
 -----------------------------------
 Replication & Full Load Application
 for multiple databases to s3.

 Find out more on our Wiki:
 https://github.com/DirksCGM/DBtoS3/wiki
"""

import os
from dotenv import load_dotenv
from dbtos3.mysql_model.db import ReplicationMethodsMySQL
from dbtos3.postgres_model.db import ReplicationMethodsPostgreSQL
from dbtos3.sqlite_model.catalogue import CatalogueMethods
from dbtos3.s3_model.service import S3ServiceMethod
from dbtos3.sentry_model.api import SentryReplicationMethod, GetSentryEventsData

APP_ROOT = os.path.join(os.path.dirname(__file__))
load_dotenv(os.path.join(APP_ROOT, '.env'))
