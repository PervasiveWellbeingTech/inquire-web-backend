from flask import Flask, request, jsonify
from werkzeug.serving import run_simple
import numpy as np
import psycopg2
from collections import defaultdict
from scipy.spatial.distance import cdist
import time

DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

app = Flask("inquire_sql")
conn = psycopg2.connect("host=localhost dbname=inquire user=postgres password=12344321")

lsh_tables = defaultdict(list)
dictionary = {}
word_vectors = np.load("/commuter/livejournal/googleEmbeddings.npy").transpose()


def load_dictionary():
    global dictionary
    print("Loading dictionary..")
    cur = conn.cursor()
    cur.execute('SELECT WORD, WORD_ID FROM word2id;')
    for word, word_id in cur.fetchall():
        dictionary[word] = word_id + 1
        dictionary[word_id + 1] = word
    print("Done.")
    print(list(dictionary.items())[:20])


def load_lsh_vectors():
    global lsh_vecs
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur.execute("SELECT ID, NUM_BITS FROM hashtables_meta where VECTOR_MODEL='W2V';")
    for table_id, num_bits in cur.fetchall():
        for bit in range(num_bits):
            cur2.execute('SELECT VECTOR FROM hash_vectors WHERE TABLE_ID=%s AND VECTOR_ID=%s;', (table_id, bit))
            for vector in cur2.fetchall():
                lsh_tables[int(table_id)].append(np.array(list(map(float, vector[0]))).reshape((300, )))


def get_vector(string):
    ids = [dictionary[word] for word in string.lower().strip().split() if word in dictionary]
    print("IDs: %s" % ids)
    if ids:
        vec = word_vectors[ids].mean(axis=0)
        # print(vec)
        return vec
    else:
        return np.zeros((300,))


def calculate_hash(vector, table_id):
    bits = [np.dot(vector, dim_vec) >= 0 for dim_vec in lsh_tables[int(table_id)]]
    hash_ = sum([(2 ** i * int(bit)) for (i, bit) in enumerate(bits)])
    print("Bits: %s. Hash: %s" % (bits, hash_))
    return hash_


def query_hashtable(q_hash, tableid):
    cur = conn.cursor()
    print("Querying table %s for hash %s.. " % (tableid, q_hash))
    start = time.time()
    cur.execute(
        """
        SELECT
            p.post_id,
            s.sent_num,
            s.sent_text,
            u.username,
            u.profile_url,
            p.post_time,
            p.mood,
            w.sent_vector
        FROM
            users u, posts p, post_sents s, sents_word2vec w, hashes h
        WHERE
            h.table_id = %s and
            h.hash = %s and
            h.post_id = p.post_id and
            h.sent_num = s.sent_num and
            s.post_id = p.post_id and
            u.userid = p.userid and
            w.post_id = h.post_id and
            w.sent_num = h.sent_num;
        """, (tableid, q_hash))
    took = time.time() - start
    all_res = [(item[:2], item[2:]) for item in cur.fetchall()]
    print("Found %s matches in %s seconds.. " % (len(all_res), took))
    return all_res


def perform_full_query(query_string, hashtable_ids, mode="and", filter_words=None, min_words=None, max_words=None):
    vec = get_vector(query_string)
    seen = dict()
    counts = defaultdict(int)
    for hashtable_id in hashtable_ids:
        t_hash = calculate_hash(vec, hashtable_id)
        res = query_hashtable(t_hash, hashtable_id)
        for primary_key, additional_data in res:
            seen[primary_key] = additional_data  # keep unique results
            counts[primary_key] += 1

    # print(list(seen.items())[:3])
    print(list(counts.items())[:20])

    cands = []
    cand_vecs = []

    if (min_words is not None) or (max_words is not None):
        print("enforcing %s <= textlen <= %s" % (min_words, max_words))

    for key, count in counts.items():
        if mode == "and" and count != len(hashtable_ids):
            continue

        val = seen[key]
        text = val[0]

        if filter_words is not None:
            for filter_word in filter_words:
                if filter_word in text:
                    continue

        if (min_words is not None) or (max_words is not None):
            textlen = len(text.split())
            if not (int(min_words or 0) <= textlen <= int(max_words or 99999)):
                continue

        cands.append(list(key) + list(val[:-1]))
        cand_vecs.append(np.array(val[-1]))
    print("Keeping %s candidates.." % len(cands))

    if len(cands) == 0:
        return []

    print("Computing similarities..")
    similarities = 1. - cdist(
        vec.reshape((1, 300)),
        np.array(cand_vecs),
        metric="cosine"
    ).reshape((len(cand_vecs),))
    cands = [cand + [sim, ] for (cand, sim) in zip(cands, similarities)]
    print("Sorting..")
    cands = sorted(cands, key=lambda item: -item[-1])
    print("All done.")
    return cands


@app.route("/query")
def query():
    print("args: %s" % request.args)
    min_words = request.args.get("minWords", None)
    max_words = request.args.get("maxWords", None)

    hashtables = request.args.getlist("hashtables")
    if len(hashtables) == 0:
        hashtables = [1, 2]
    print(hashtables)
    mode = request.args.get("mode", "and")

    filter_words = request.args.get("filter", None)
    if filter_words:
        filter_words = [w.strip() for w in filter_words.split(",")]
    top = request.args.get("top")
    query_string = request.args.get("data")

    res = perform_full_query(
        query_string,
        hashtable_ids=hashtables,
        mode=mode,
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
    load_dictionary()
    load_lsh_vectors()
    run_simple("0.0.0.0", port=8080, application=app)
