''' WSGI Initializer '''
import logging
from main import application
from flask import current_app
from service.request_handler import FLPORequestHandler

if __name__ == "__main__":
    # logging.basicConfig(filename='werkzeug.log', level=logging.ERROR)
    logging.basicConfig(stream=sys.stderr, level=current_app.config["LOG_LEVEL"])
    LOGGER = logging.getLogger('werkzeug')
    LOGGER.setLevel(logging.ERROR)
    application.run(request_handler=FLPORequestHandler)
