from werkzeug.serving import run_simple
from inquire_sql_backend.server_annoy import app

if __name__ == '__main__':
    run_simple("0.0.0.0", port=9000, application=app)
