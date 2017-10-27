import logging
import time

import psycopg2
from annoy import AnnoyIndex
from flask import Flask, request, jsonify

from inquire_sql_backend.semantics.embeddings.vectors import vector_embed_sentence

logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",level=logging.DEBUG)
log = logging.getLogger(__name__)
DEC2FLOAT = psycopg2.extensions.new_type(psycopg2.extensions.DECIMAL.values, 'DEC2FLOAT', lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

INDEXES = {
    "glove": (
        "/commuter/pdowling/annoy-index/new_10.0M_livejournal_glove_200trees.indx",
        "/commuter/pdowling/annoy-index/new_10.0M_annoy_to_sql_ids_glove_200.txt"
    ),
    "lstm": (
        "/commuter/pdowling/annoy-index/new_10.0M_livejournal_glove_200trees.indx",
        "/commuter/pdowling/annoy-index/new_10.0M_annoy_to_sql_ids_glove_200.txt"
    )
}

app = Flask("inquire_sql")
conn = psycopg2.connect("host=localhost dbname=inquire_newparse user=postgres password=12344321")

_index = None
_id_map = None


def get_index_and_id_map(method="glove"):
    global _index, _id_map
    if _index is None:
        _index = AnnoyIndex(300, metric="angular")

        _index.load(INDEXES[method][0])
        _id_map = {}
        with open(INDEXES[method][1], "r") as inf:
            for line in inf:
                id_, post_id, sent_num = list(map(int, line.strip().split()))
                _id_map[id_] = (post_id, sent_num)
    return _index, _id_map


def query_hashtable(query_string, nns=100, search_k=1000000, method="glove"):
    query_vector = vector_embed_sentence(query_string, method=method)

    cur = conn.cursor()
    index, id_map = get_index_and_id_map(method=method)
    result_ids = [
        (id_map[id_], 1. - dist)
        for id_, dist
        in zip(*index.get_nns_by_vector(query_vector, nns, search_k=search_k, include_distances=True))
    ]
    result_ids = sorted(result_ids, key=lambda kv: -kv[1])
    log.debug("Annoy returned %s items" % len(result_ids))
    annoy_res, dists = zip(*result_ids)
    start = time.time()

    log.debug("Running separate queries for post lookup..")
    all_res = []
    for post_id, sent_num in annoy_res:
       cur.execute(
           """
           SELECT
               p.lj_post_id,
               s.sent_num,
               s.sent_text,
               u.username,
               p.post_time,
               p.mood
           FROM
               users u, posts p, post_sents s
           WHERE
               s.post_id = p.post_id and
               p.userid = u.userid and
               s.post_id = %s and
               s.sent_num = %s;
           """, (post_id, sent_num))

       all_res.append(cur.fetchall()[0])

    took = time.time() - start
    print("Found %s matches in %s seconds.. " % (len(all_res), took))
    return all_res, dists


def perform_full_query(
        query_string, method="glove", nns=100, search_k=500000, filter_words=None, min_words=None, max_words=None
):
    res, dists = query_hashtable(query_string, nns=nns, search_k=search_k, method=method)

    log.debug("Found %s results in hashtables" % len(res))

    cands = []

    if (min_words is not None) or (max_words is not None):
        print("enforcing %s <= textlen <= %s" % (min_words, max_words))

    for row, dist in sorted(zip(res, dists), key=lambda item: item[1]):
        text = row[2]
        remove = False

        if filter_words is not None:
            for filter_word in filter_words:
                if filter_word in text:
                    remove = True

        if (min_words is not None) or (max_words is not None):
            textlen = len(text.split())
            if not (int(min_words or 0) <= textlen <= int(max_words or 99999)):
                remove = True

        if not remove:
            cands.append(list(row) + [dist])

    print("Keeping %s candidates.." % len(cands))

    if len(cands) == 0:
        return []

    print("All done.")
    return cands


@app.route("/query")
def query():
    print("args: %s" % request.args)
    min_words = request.args.get("minWords", None)
    max_words = request.args.get("maxWords", None)
    # nns = request.args.get("nns", 100)

    filter_words = request.args.get("filter", None)
    if filter_words:
        filter_words = [w.strip().lower() for w in filter_words.split(",")]
    top = int(request.args.get("top"))
    top = min(top, 1000)
    log.debug("Top is %s" % top)
    log.debug("Filter words are %s" % (filter_words,))

    search_k = request.args.get("search_k", 1000000)

    any_filters = filter_words is not None or min_words is not None or max_words is not None
    query_string = request.args.get("data")

    res = perform_full_query(
        query_string,
        nns=top * 2 if any_filters else top,
        search_k=search_k,
        filter_words=filter_words,
        min_words=min_words,
        max_words=max_words
    )
    res = res[-top:]
    if res:
        print(res[0])
        post_ids, sent_nums, sent_texts, usernames, post_times, moods, similarities = zip(*res)
        urls = [
            "http://%s.livejournal.com/%s.html" % (username, post_id)
            for (username, post_id) in zip(usernames, post_ids)
        ]

        return jsonify(
            {
                "result_count": len(urls),
                "query": query_string,
                "query_results": [sent_texts],
                "cosine_similarity": similarities,
                "emotion": [moods],
                "url": [urls]
            }
        )
    else:
        return jsonify({"urls": [], "sents": [], "timestamps": [], "moods": []})
