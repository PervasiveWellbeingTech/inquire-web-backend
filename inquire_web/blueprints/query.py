from __future__ import print_function
from flask import Blueprint, request
import requests
import logging
log = logging.getLogger(__name__)

query_blueprint = Blueprint("query", __name__, url_prefix="/query")

query_api_path = None


def init_query_blueprint(api_path):
    global query_api_path
    query_api_path = api_path


@query_blueprint.route("/default", methods=["POST"])
def execute_search():
    log.debug("Forwarding search query to %s" % query_api_path)
    query_data = request.json["query"]
    new_query = query_api_path + "query"
    return requests.get(new_query, params=query_data).json()
