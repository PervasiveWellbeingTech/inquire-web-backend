from collections import defaultdict

from inquire_sql_backend.query.db import retrieve_user_sents
from inquire_sql_backend.semantics.embeddings.vector_sim import vector_embed_sentence
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import bottleneck
import logging

log = logging.getLogger(__name__)


def query_user_sents(username, query_vector, model="default", dataset="livejournal", top=None):
    sents = retrieve_user_sents(username, dataset=dataset)

    log.debug("DB (%s) returned %s sentences for user %s. Evaluating similarities.." % (dataset, len(sents), username))
    vecs = [vector_embed_sentence(sent["sent_text"], model=model) for sent in sents]
    sents_vecs = [(sent, vec) for sent, vec in zip(sents, vecs) if vec is not None]
    if not sents_vecs:
        return []
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

    return list(top_sents)


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

