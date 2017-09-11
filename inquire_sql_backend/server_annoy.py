import psycopg2
import numpy as np
from annoy import AnnoyIndex
import time
import logging
from flask import Flask, request, jsonify
from werkzeug.serving import run_simple

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
    level=logging.DEBUG
)
log = logging.getLogger(__name__)

DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

app = Flask("inquire_sql")
conn = psycopg2.connect("host=localhost dbname=inquire user=postgres password=12344321")

dictionary = None
word_vectors = np.load("/commuter/livejournal/googleEmbeddings.npy").transpose()


def load_dictionary():
    global dictionary
    if dictionary is None:
        dictionary = {}
        log.debug("Loading dictionary..")
        cur = conn.cursor()
        cur.execute('SELECT WORD, WORD_ID FROM word2id;')
        for word, word_id in cur.fetchall():
            dictionary[word] = word_id + 1
            dictionary[word_id + 1] = word
        log.debug("Done.")
        # print(list(dictionary.items())[:20])
    return dictionary


def get_vector(string):
    load_dictionary()
    ids = [dictionary[word] for word in string.lower().strip().split() if word in dictionary]
    log.debug("IDs: %s" % ids)
    if ids:
        vec = word_vectors[ids].mean(axis=0)
        # log.debug(vec)
        return vec
    else:
        return np.zeros((300,))

_index = None
_id_map = None
def get_index_and_id_map(trees=30):
    global _index, _id_map
    if _index is None:
        _index = AnnoyIndex(300, metric="angular")
        _index.load("/commuter/pdowling/annoy-index/livejournal_w2v_%strees.indx" % trees)
        _id_map = {}
        with open("/commuter/pdowling/annoy-index/annoy_to_sql_ids.txt", "r") as inf:
            for line in inf:
                id_, post_id, sent_num = list(map(int, line.strip().split()))
                _id_map[id_] = (post_id, sent_num)
    return _index, _id_map


def query_hashtable(query_vector, nns=100, search_k=1000000, test=True):
    cur = conn.cursor()
    index, id_map = get_index_and_id_map()
    result_ids = [
        (id_map[id_], dist)
        for id_, dist
        in zip(*index.get_nns_by_vector(query_vector, nns, search_k=search_k, include_distances=True))
    ]
    log.debug("Annoy returned %s items" % len(result_ids))
    annoy_res, dists = zip(*result_ids)
    start = time.time()

    log.debug("Running separate queries..")
    all_res = []
    for post_id, sent_num in annoy_res:
       cur.execute(
           """
           SELECT
               p.post_id,
               s.sent_num,
               s.sent_text,
               u.username,
               u.profile_url,
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


def perform_full_query(query_string, nns=100, search_k=1000000, filter_words=None, min_words=None, max_words=None):
    vec = get_vector(query_string)

    res, dists = query_hashtable(vec, nns=nns, search_k=search_k)
    log.debug("Found %s results in hashtables" % len(res))

    cands = []

    if (min_words is not None) or (max_words is not None):
        print("enforcing %s <= textlen <= %s" % (min_words, max_words))

    for row, dist in sorted(zip(res, dists), key=lambda item: item[1]):
        text = row[2]

        if filter_words is not None:
            for filter_word in filter_words:
                if filter_word in text:
                    continue

        if (min_words is not None) or (max_words is not None):
            textlen = len(text.split())
            if not (int(min_words or 0) <= textlen <= int(max_words or 99999)):
                continue

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
        filter_words = [w.strip() for w in filter_words.split(",")]
    top = int(request.args.get("top"))

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
    res = res[:top]
    if res:
        print(res[0])
        post_ids, sent_nums, sent_texts, usernames, profile_urls, post_times, moods, similarities = zip(*res)
        urls = [prof_url + str(post_id) + ".html" for (prof_url, post_id) in zip(profile_urls, post_ids)]

        return jsonify(
            {"urls": urls, "sents": sent_texts, "timestamps": post_times, "moods": moods, "sims": similarities}
        )
    else:
        return jsonify({"urls": [], "sents": [], "timestamps": [], "moods": []})

if __name__ == '__main__':
    run_simple("0.0.0.0", port=9000, application=app)
