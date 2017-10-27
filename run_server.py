from werkzeug.serving import run_simple
# from inquire_sql_backend.server_annoy import app
from inquire_sql_backend.server_nms import app

if __name__ == '__main__':
    run_simple("0.0.0.0", port=9001, application=app)
