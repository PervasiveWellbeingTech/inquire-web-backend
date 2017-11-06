import logging
import time

from flask import Flask, request, jsonify

from inquire_sql_backend.query.db import retrieve_sent_metadata, retrieve_sent_context_metadata
from inquire_sql_backend.query.user_query import query_user_sents, retrieve_all_user_posts
from inquire_sql_backend.query.util import apply_filters, parse_query_params
from inquire_sql_backend.semantics.embeddings.vector_sim import vector_embed_sentence
from inquire_sql_backend.semantics.nearest_neighbors.NMSLibIndex import get_nms_index

logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s", level=logging.DEBUG)
log = logging.getLogger(__name__)

app = Flask("inquire_sql")


def perform_full_query(query_vector, model="default", dataset="livejournal", nns=100):
    start = time.time()
    # query_vector = vector_embed_sentence(query_string, method=method)
    # Run the ANN query and sort
    index = get_nms_index(model=model, dataset=dataset)
    if index is None:
        raise ValueError("No index found for model %s and dataset %s" % (model, dataset))

    ann_query_results = index.query(query_vector, nns)
    result_ids = [(sent_pkey, float(dist)) for (sent_pkey, dist) in zip(*ann_query_results)]
    result_ids = sorted(result_ids, key=lambda kv: -kv[1])
    nms_res, dists = zip(*result_ids)

    # Retrieve full data from DB
    t1 = time.time() - start
    log.debug("ANN search returned %s items in %s seconds, now running db queries for content.." % (len(nms_res), t1))

    nearest_neighbors_full = []
    for (post_id, sent_num), dist in zip(nms_res, dists):
        res_dict = retrieve_sent_metadata(post_id, sent_num, dataset=dataset)
        res_dict["similarity"] = float(str(1. - dist)[:4])
        nearest_neighbors_full.append(res_dict)

    return nearest_neighbors_full


@app.route("/query")
def query():
    print("args: %s" % request.args)
    any_filters, filter_words, max_words, min_words, query_string, top, model, dataset = parse_query_params()

    try:
        query_vector = vector_embed_sentence(query_string, model=model)
    except KeyError:
        return jsonify({"error": "unknown vector model"})

    results_to_keep = []
    if query_vector is not None:

        start = time.time()
        unfiltered_nearest_neighbors = perform_full_query(
            query_vector,
            nns=top * 2 if any_filters else top,  # TODO there's almost definitely a better way of setting nns
            dataset=dataset,
            model=model
        )
        took = time.time() - start

        print("Found %s matches in %s seconds total, now filtering.. " % (len(unfiltered_nearest_neighbors), took))
        results_to_keep = apply_filters(unfiltered_nearest_neighbors, max_words, min_words, filter_words, top)

        if results_to_keep:
            print(results_to_keep[0])

        # TODO re-try with more nns if there are 0 matches

    res = {
        "result_count": len(results_to_keep),
        "query": query_string,
        "query_results": results_to_keep
    }
    return jsonify(res)


@app.route("/user/<username>")
def get_user(username):
    dataset = request.args.get("dataset", "livejournal")
    return jsonify(retrieve_all_user_posts(username, dataset=dataset))


@app.route("/user_query/<username>")
def query_user(username):
    print("args: %s" % request.args)
    any_filters, filter_words, max_words, min_words, query_string, top, model, dataset = parse_query_params()

    try:
        query_vector = vector_embed_sentence(query_string, model=model)
    except KeyError:
        return jsonify({"error": "unknown vector model"})

    start = time.time()
    filtered_nearest_neighbors = query_user_sents(
        username,
        query_vector, top=top * 2 if any_filters else top,  # TODO there's almost definitely a better way of setting nns
        model=model,
        dataset=dataset
    )
    took = time.time() - start

    print("Found %s matches in %s seconds total, now filtering.. " % (len(filtered_nearest_neighbors), took))
    results_to_keep = apply_filters(filtered_nearest_neighbors, max_words, min_words, filter_words, top)

    if filtered_nearest_neighbors:
        print(results_to_keep[0])

    res = {
        "result_count": len(results_to_keep),
        "query": query_string,
        "username": username,
        "query_results": results_to_keep
    }
    return jsonify(res)


@app.route("/contexts", methods=["POST"])
def get_contexts():
    print("args: %s" % request.args)
    posted_data = request.get_json(force=True)
    window = posted_data.get("window", None)
    dataset = posted_data.get("dataset", "livejournal")
    res_windows = []
    for post_id, sent_num in posted_data["sents"]:
        res_windows.append(retrieve_sent_context_metadata(post_id, sent_num, window, dataset=dataset))
    return jsonify(
        {
            "result_count": len(res_windows),
            "query_results": res_windows
        }
    )
