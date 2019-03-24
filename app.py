from flask import Flask
from celery import Celery

from flask_pymongo import PyMongo
from flask_session import Session

app = Flask(__name__)
SESSION_TYPE = 'redis'
app.config.from_object(__name__)
Session(app)

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

app.config['ANL_DBNAME'] = 'anl_database'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
mongodb = PyMongo(app, config_prefix='ANL')
