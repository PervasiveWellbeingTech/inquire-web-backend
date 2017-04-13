from __future__ import print_function
from flask import Blueprint, request, jsonify
from collections import defaultdict
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
    # {minWords: "5", maxWords: "30", filter: "", top: "100", data: "here's some text"}
    res = run_semantic_search(query_data)
    return jsonify(res)


@query_blueprint.route("/user_overlap", methods=["POST"])
def execute_user_overlap_search():
    queries = request.json["queries"]

    def get_user_sents(query_data):
        res = run_semantic_search(query_data)
        users_sents = defaultdict(list)
        for url, sent, relevance in zip(res["url"][0], res["query_results"][0], res["cosine_similarity"]):
            user = url.split(".")[0][len("http://"):]
            users_sents[user].append((sent, relevance))  # TODO relevance is not re-weighted at any point currently

        return users_sents

    all_user_sents = [get_user_sents(query_data) for query_data in queries]
    all_user_names = [d.values() for d in all_user_sents]
    overlapping_names = list(set(all_user_names[0]).intersection(*all_user_names))
    result = {
        "users": overlapping_names,
        "sents": [all_user_sents[user] for user in overlapping_names]
    }
    return jsonify(result)


def run_semantic_search(query_data):
    new_query = query_api_path + "query"
    res = requests.get(new_query, params=query_data).json()
    log.debug("returning query results!")
    return res
