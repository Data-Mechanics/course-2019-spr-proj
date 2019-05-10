
from flask import Flask


def create_app(config_filename=None):
    app = Flask(__name__)
    if config_filename:
        app.config.from_pyfile(config_filename)

    from web.views import cs504

    app.register_blueprint(cs504)

    return app
