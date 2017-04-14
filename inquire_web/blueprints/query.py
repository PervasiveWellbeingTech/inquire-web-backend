from __future__ import print_function
from flask import Blueprint, request, jsonify
from collections import defaultdict
import sqlite3
import datetime
import requests
import logging
log = logging.getLogger(__name__)

query_blueprint = Blueprint("query", __name__, url_prefix="/query")

query_api_path = None


db_file = 'log_db.sqlite'
conn = sqlite3.connect(db_file)
c = conn.cursor()

parms = ("'%s'," * 6)[:-1]
sql = "INSERT INTO query_log VALUES (%s)" % (parms)


def init_query_blueprint(api_path):
    global query_api_path
    query_api_path = api_path


@query_blueprint.route("/default", methods=["POST"])
def execute_search():
    log.debug("Forwarding search query to %s" % query_api_path)
    query_data = request.json["query"]
    # {minWords: "5", maxWords: "30", filter: "", topN: "100", data: "here's some text"}

    # Save the metadata into the database
    current_date = str(datetime.datetime.now())

    # sample_query = (current_date, "test query", 0, 10, "", 100)
    row = (current_date, query_data['data'], query_data['minWords'], query_data['maxWords'], query_data['topN'])
    c.execute(sql % row)

    print('Logged info to DB!')
    # query the scala backend
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
    new_query = query_api_path + "query"
    res = requests.get(new_query, params=query_data).json()
    log.debug("returning query results!")
    return res
