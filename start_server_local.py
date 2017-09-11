import datetime
import logging
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
    level=logging.DEBUG
)
rootlog = logging.getLogger()
rootlog.addHandler(logging.FileHandler(filename="logs/inquire_%s.log" % datetime.datetime.now().isoformat()))

log = logging.getLogger(__name__)

from werkzeug.serving import run_simple
from inquire_web.blueprints.query import init_query_blueprint
from flask import Flask, send_from_directory
from werkzeug.wsgi import DispatcherMiddleware


if __name__ == "__main__":
    with open("backend_servers.txt", "r") as inf:
        servers = [line.strip() for line in inf.readlines() if line.strip()]
        init_query_blueprint(servers)

    from inquire_web.server import app

    # <<<<<<< HEAD:backend-server-python/start_server_local.py
    # =======
    #
    # def build_app():
    #     app = Flask(__name__.split(".")[0])
    #     app.debug = True
    # >>>>>>> 18382fce5853a1473437bd9f26e5223c29c65d67:backend-server-python/start_server.py
    static_app = Flask(__name__.split(".")[0] + "_static")
    static_app.debug = True
    app.debug = True

    # This is for local debugging and development, so we need to serve static files
    @static_app.route("/", defaults={'path': 'index.html'})
    @static_app.route('/<path:path>')
    def server_static(path):
        return send_from_directory('static', path)

    local_app = DispatcherMiddleware(
        static_app,
        {
            "/api": app
        }
    )
    local_app.config = {}
    local_app.debug = True
    # <<<<<<< HEAD:backend-server-python/start_server_local.py
    log.debug("Running server..")
    run_simple('0.0.0.0', 80, local_app)
    #  =======
    #
    #      return local_app
    #
    #
    #  if __name__ == "__main__":
    #      app = build_app()
    #      run_simple('0.0.0.0', 8000, app)
    #  >>>>>>> 18382fce5853a1473437bd9f26e5223c29c65d67:backend-server-python/start_server.py
