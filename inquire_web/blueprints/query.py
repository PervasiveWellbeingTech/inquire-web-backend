from __future__ import print_function
from flask import Blueprint, request, jsonify
from collections import defaultdict
import requests
import logging

log = logging.getLogger(__name__)

query_blueprint = Blueprint("query", __name__, url_prefix="/query")

query_api_paths = None
current_path = 0


def init_query_blueprint(api_paths):
    global query_api_paths
    query_api_paths = api_paths


def select_next_api_path():  # TODO this is technically not atomic
    global query_api_paths, current_path
    with open("selected_backend.txt", "r+") as f:
        current_path = int(f.read().strip())
        f.seek(0)
        log.debug("Read current_path = %s from file" % current_path)
        f.write("%s" % ((current_path + 1) % len(query_api_paths)))
        f.truncate()

    return query_api_paths[current_path]


@query_blueprint.route("/default", methods=["POST"])
def execute_search():
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
            # TODO relevance is not re-weighted at any point currently
            users_sents[user].append((query_data["name"], sent, relevance))

        return users_sents

    all_user_sents = [get_user_sents(query_data) for query_data in queries]
    user_sents_combined = defaultdict(list)

    all_user_names = []
    for user_sents in all_user_sents:
        all_user_names.append(user_sents.keys())
        for key, val in user_sents.items():
            user_sents_combined[key] += val

    overlapping_names = list(set(all_user_names[0]).intersection(*all_user_names))
    result = {
        "users": overlapping_names,
        "sents": [user_sents_combined[user] for user in overlapping_names]
    }
    return jsonify(result)


def run_semantic_search(query_data):
    api_path = select_next_api_path()
    log.debug("Forwarding search query to %s" % api_path)
    new_query = api_path + "query"
    res = requests.get(new_query, params=query_data).json()
    log.debug("returning query results!")
    return res
