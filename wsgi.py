__author__ = 'dowling'
import datetime
import logging
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
    level=logging.DEBUG
)
rootlog = logging.getLogger()
rootlog.addHandler(logging.FileHandler(filename="inquire_%s.log" % datetime.datetime.now().isoformat()))

log = logging.getLogger(__name__)
from inquire_web.server import app
from inquire_web.blueprints.query import init_query_blueprint
from werkzeug.wsgi import DispatcherMiddleware

with open("backend_servers.txt", "r") as inf:
    servers = [line.strip() for line in inf.readlines() if line.strip()]
    init_query_blueprint(servers)

def placeholder(env, resp):
    resp(b'200 OK', [(b'Content-Type', b'text/plain')])
    return [b'Hello WSGI World']

# TODO not sure if we need /api prefix
application = DispatcherMiddleware(placeholder, {"/api": app})

if __name__ == "__main__":
    application.run()
