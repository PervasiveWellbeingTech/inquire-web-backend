from collections import defaultdict

import time

from inquire_sql_backend.query.db import retrieve_user_sents, retrieve_all_sents
from inquire_sql_backend.semantics.embeddings.vector_sim import vector_embed_sentence, vector_embed_sentence_batch
from sklearn.metrics.pairwise import cosine_similarity
from gensim.utils import grouper
import numpy as np
import bottleneck
import logging
import heapq

log = logging.getLogger(__name__)

TIE_BREAKER = 0


def query_brute_force(query_vectors, model="default", dataset="livejournal", top=None, percentage=0.01):
    # TODO make limit a percentage instead?
    global TIE_BREAKER

    approx_total = 1000000000 if dataset == "livejournal" else 126269098

    if 0 < percentage <= 1:
        limit = int(percentage * approx_total)
    else:
        limit = int(percentage)

    if top is None:
        top = 2000

    query_i_map = []
    query_vecs_matrix = []
    # Just populate empty Qs
    raw_heapqs = []
    for query_i, vec in enumerate(query_vectors):
        one_q = []
        if vec is not None:
            for _ in range(top):
                one_q.append((0, TIE_BREAKER, None))
                TIE_BREAKER += 1

            query_i_map.append(query_i)  # we use this to skip over empty queries but still be able to do matrix math
            query_vecs_matrix.append(vec)
        raw_heapqs.append(one_q)

    query_vecs_matrix = np.array(query_vecs_matrix)
    query_vecs_matrix /= np.linalg.norm(query_vecs_matrix, axis=1)[:, np.newaxis]

    log.debug("Running brute force search on %s queries." % len(query_vectors))
    cur = retrieve_all_sents(dataset=dataset, limit=limit)
    start = None
    chunksize = 10000
    for idx, chunk in enumerate(grouper(cur, chunksize)):
        if start is None:
            start = time.time()
        else:
            took_sofar = time.time() - start
            done = idx * chunksize
            percent = (done / approx_total) * 100
            speed = int(done / took_sofar)
            log.debug("Finished %s%% (%s sents, %s secs, %s sents / second).." % (percent, done, took_sofar, speed))

            if done >= 100000:
                break

        post_ids, sent_nums, texts = zip(*chunk)
        vecs = vector_embed_sentence_batch(texts, model=model)

        post_ids_sent_nums_texts_vecs = [
            (post_id, sent_num, text, vec)
            for post_id, sent_num, text, vec
            in zip(post_ids, sent_nums, texts, vecs)
            if vec is not None
        ]

        post_ids, sent_nums, texts, vecs = zip(*post_ids_sent_nums_texts_vecs)
        vecs = np.array(vecs)
        vecs /= np.linalg.norm(vecs, axis=1)[:, np.newaxis]

        similarities = np.dot(query_vecs_matrix, vecs.T)

        for query_i, query_similarities in zip(query_i_map, similarities):
            assert len(raw_heapqs[query_i]) == top  # TODO remove
            for post_id, sent_num, text, sent_sim in zip(post_ids, sent_nums, texts, query_similarities):
                if sent_sim > raw_heapqs[query_i][0][0]:
                    sent_obj = (post_id, sent_num, text, float(sent_sim))  # TODO we should lookup the rest of the data here
                    heapq.heapreplace(raw_heapqs[query_i], (sent_sim, TIE_BREAKER, sent_obj))
                    TIE_BREAKER += 1

    cur.close()

    final_res = []
    for raw_heapq in raw_heapqs:
        res = [heapq.heappop(raw_heapq)[-1] for _ in range(len(raw_heapq))]
        res = [item for item in res if item is not None]
        res.reverse()
        final_res.append(res)

    return final_res


def query_user_sents(username, query_vector_s, model="default", dataset="livejournal", top=None, _batch=False):
    sents = retrieve_user_sents(username, dataset=dataset)
    if not _batch:
        query_vector_s = [query_vector_s]
    all_top_sents = []
    log.debug("DB (%s) returned %s sentences for user %s. Evaluating similarities.." % (dataset, len(sents), username))

    for query_vector in query_vector_s:
        if query_vector is None:
            all_top_sents.append([])
            continue

        vecs = [vector_embed_sentence(sent["sent_text"], model=model) for sent in sents]
        sents_vecs = [(sent, vec) for sent, vec in zip(sents, vecs) if vec is not None]
        if not sents_vecs:
            all_top_sents.append([])
            continue

        sents, vecs = list(map(np.array, zip(*sents_vecs)))
        sims = cosine_similarity(query_vector[np.newaxis, :], vecs)[0]
        neg_sims = -sims
        if top is None or top >= len(neg_sims):
            topargsort = np.argsort(neg_sims)
        else:
            topargs = bottleneck.argpartition(neg_sims, top)[:top]
            topargsort = topargs[np.argsort(neg_sims[topargs])]

        top_sents, top_sims = sents[topargsort], sims[topargsort]
        for sent, sim in zip(top_sents, top_sims):
            sent["similarity"] = sim
        all_top_sents.append(list(top_sents))

    if not _batch:
        return all_top_sents[0]
    else:
        return all_top_sents


def retrieve_all_user_posts(username, dataset="livejournal"):
    sents = retrieve_user_sents(username, dataset=dataset)
    posts = defaultdict(list)
    for sent in sents:
        posts[sent["ext_post_id"]].append(sent)

    for post_id, sentlist in posts.items():
        sentlist.sort(key=lambda s: s["sent_num"])

    post_dicts = [
        {
            "ext_post_id": post_id,
            "post_time": sentlist[0]["post_time"],
            "username": sentlist[0]["username"],
            "url": sentlist[0]["url"],
            "sents": [{"sent_text": s["sent_text"], "sent_num": s["sent_num"]} for s in sentlist],
        }
        for post_id, sentlist in sorted(posts.items(), key=lambda kv: kv[0])
    ]

    return post_dicts
