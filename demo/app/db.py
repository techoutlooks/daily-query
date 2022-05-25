import flask
from flask_pymongo import PyMongo


mongo = PyMongo()
mongo.init_app(flask.current_app)


def get_db():
    return mongo.db
