import sys
import os
import logging
from apiflask import APIFlask, Schema, abort, APIBlueprint
from apiflask.fields import Integer, String
from apiflask.validators import Length, OneOf
from flask import request, jsonify
from flask_cors import CORS
from dbt_pr_init_api import setup_routes as setup_pr_init_routes
from dbt_pr_mgmt_api import setup_routes as setup_pr_mgmt_routes
from config import Config
from security import auth
from werkzeug.middleware.proxy_fix import ProxyFix
from api_docs import configure_api_docs
#from db import dbNavigator

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

def configure_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
                        filename="app.log",
                        filemode='a')
    
    stdout_logger = logging.getLogger('STDOUT')
    sl = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = sl

    stderr_logger = logging.getLogger('STDERR')
    sl = StreamToLogger(stderr_logger, logging.ERROR)
    sys.stderr = sl

def create_app():
    configure_logging()
    app = APIFlask(__name__, title='Fast.BI DBT Project Initialization API', docs_path='/api/v3/docs', version='3.0')
    app.wsgi_app = ProxyFix(
        app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1
    )
    # Enable CORS support
    CORS(app)

    configure_api_docs(app)

    app.config.from_object(Config)
    app.config['CACHE_TYPE'] = 'SimpleCache'

    # Debug - disabling database connection as for now is not used.
    # db_config = {
    #     'dbname': app.config['DB_NAME'],
    #     'user': app.config['DB_USER'],
    #     'password': app.config['DB_PASSWORD'],
    #     'host': app.config['DB_HOST'],
    #     'port': app.config['DB_PORT']
    # }

    # metadata_collector = dbNavigator(db_config)
    # app.metadata_collector = metadata_collector

    # Create and register the blueprint
    api_v3_bp = APIBlueprint('api v3 blueprint', __name__, url_prefix='/api/v3')
    setup_pr_init_routes(api_v3_bp)
    setup_pr_mgmt_routes(api_v3_bp)
    app.register_blueprint(api_v3_bp)

    @app.errorhandler(404)
    def handle_404_error(e):
        return jsonify({'error': 'Resource not found'}), 404

    @app.errorhandler(500)
    def handle_500_error(e):
        return jsonify({'error': 'Internal server error'}), 500

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=8888)
