import flask
from flask import render_template, flash

from daily_query.mongo import MongoDaily
from demo.app.db import get_db


@flask.current_app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@flask.current_app.route("/posts")
def show_posts():
    try:
        db = MongoDaily(get_db())
        posts = list(db.search(flatten=True))
    except Exception as e:
        flash(e)
        raise RuntimeError(e)
    return render_template('posts/index.html', posts=posts)


@flask.current_app.route('/post/<int:post_id>')
def show_post(post_id):
    pass

