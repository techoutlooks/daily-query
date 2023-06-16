from datetime import datetime
from typing import Mapping, Any

import flask
from flask import render_template, flash

from daily_query.mongo import MongoDaily, Collection
from demo.app.db import get_db


db = MongoDaily(get_db())


@flask.current_app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@flask.current_app.route("/posts")
def show_posts():
    try:
        posts = list(db.search(flatten=True))
    except Exception as e:
        flash(e)
        raise RuntimeError(e)
    return render_template('posts/list.html', posts=posts)


@flask.current_app.route('/post/<int:post_id>')
def show_post(post_id):
    pass


def create_or_update_post(update: dict, **kwargs):
    """ Create or edit a post """

    if not kwargs:
        date = str(datetime.today().date())
        match = {'_id': None}   # just a trick not to match anything
        day = Collection(date)
        day.update_one(match, pipeline, upsert=True)


