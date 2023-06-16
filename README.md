
# daily-query

Helpers for opinionated NoSQL queries that assumes that data is stored in collections named after the creation date. 
Is a wrapper around major NoSQL engines.


## Dev setup

```shell
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## API playground

```python
import pymongo
from daily_query.mongo import MongoDaily

mongo = pymongo.MongoClient('mongodb://localhost:27017/scraped_news_db')
db = MongoDaily(mongo.db)

# search posts
posts = list(db.search(flatten=True))
```

## Run the demo flask app

```shell
python setup.py develop
pip install Flask flask-pymongo

export FLASK_APP=demo/app
export FLASK_ENV=development
flask run
````

Head your browser at [http://localhost:5000/posts](http://localhost:5000/posts)

## Dependencies

MongoDB instance running at `mongodb://localhost:27017/scraped_news_db`
and loaded with data.

## Misc

Inspired of:
- https://flask.palletsprojects.com/en/2.1.x/tutorial/blog/
- https://codingshell.com/flask/mongo/flask-mongodb-tutorial/