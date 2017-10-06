import psycopg2
import time
import logging
from flask import Flask, request, jsonify
from inquire_sql_backend.nearest_neighbors.NMSLibIndex import NMSLibIndex
from inquire_sql_backend.embeddings.vectors import vector_embed_sentence

logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s", level=logging.DEBUG)
log = logging.getLogger(__name__)
DEC2FLOAT = psycopg2.extensions.new_type(psycopg2.extensions.DECIMAL.values, 'DEC2FLOAT',
                                         lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

INDEXES = {
    "glove": "/commuter/inquire-nmslib-index/nms_10.0M_livejournal_glove.idx",
    "lstm": None  # TODO
}

app = Flask("inquire_sql")
conn = psycopg2.connect("host=localhost dbname=inquire_newparse user=postgres password=12344321")

_index = None


def get_nms_index(method="glove"):
    global _index
    if _index is None:
        _index = NMSLibIndex.load(INDEXES[method])

    return _index


def query_hashtable(query_string, nns=100, method="glove"):
    query_vector = vector_embed_sentence(query_string, method=method)

    start = time.time()
    cur = conn.cursor()
    index = get_nms_index(method=method)
    result_ids = [
        ((post_id, sent_num), float(dist))
        for ((post_id, sent_num), dist)
        in zip(*index.query(query_vector, nns))
    ]
    result_ids = sorted(result_ids, key=lambda kv: -kv[1])
    log.debug("NMSLib returned %s items" % len(result_ids))
    nms_res, dists = zip(*result_ids)

    log.debug("Running separate queries for post lookup..")
    all_res = []
    for post_id, sent_num in nms_res:
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
        query_string, method="glove", nns=100, filter_words=None, min_words=None, max_words=None
):
    res, dists = query_hashtable(query_string, nns=nns, method=method)

    log.debug("Found %s results in hashtables" % len(res))

    cands = []

    if (min_words is not None) or (max_words is not None):
        print("enforcing %s <= textlen <= %s" % (min_words, max_words))

    for row, dist in zip(res, dists):
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

    any_filters = filter_words is not None or min_words is not None or max_words is not None
    query_string = request.args.get("data")

    res = perform_full_query(
        query_string,
        nns=top * 2 if any_filters else top,
        filter_words=filter_words,
        min_words=min_words,
        max_words=max_words
    )
    res = res[:top]
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
