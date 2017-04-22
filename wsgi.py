__author__ = 'dowling'
from inquire_web.server import app
from werkzeug.wsgi import DispatcherMiddleware


def placeholder(env, resp):
    resp(b'200 OK', [(b'Content-Type', b'text/plain')])
    return [b'Hello WSGI World']

# TODO not sure if we need /api prefix
application = DispatcherMiddleware(placeholder, {"/api": app})

if __name__ == "__main__":
    application.run()
