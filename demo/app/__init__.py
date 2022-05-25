from flask import Flask


def create_app():
    app = Flask(__name__)
    app.config['MONGO_URI'] = 'mongodb://localhost:27017/scraped_news_db'
    app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

    with app.app_context():
        from . import routes

    return app


app = create_app()
