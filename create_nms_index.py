import itertools
import logging
from multiprocessing import pool

import psycopg2
import time

from inquire_sql_backend.query.db import conns
from inquire_sql_backend.query.util import is_russian
from inquire_sql_backend.semantics.embeddings.vector_sim import vector_embed_sentence_batch
from inquire_sql_backend.semantics.embeddings.util import tokenize
from inquire_sql_backend.semantics.nearest_neighbors.NMSLibIndex import NMSLibIndex

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


def populate_nmslib_index(fname, count=None, userids=None, model="default", dataset="livejournal", do_tokenization=True):
    index = NMSLibIndex()
    p = pool.Pool(processes=14)

    filtered = iterate_sents_from_db(count=count, userids=userids, dataset=dataset)
    log.debug("Populating index..")

    db_cursor, curit2 = itertools.tee(filtered)
    texts = (row[-1] for row in curit2)

    if do_tokenization:
        tokens_it = p.imap(tokenize, texts, chunksize=1000)
    else:
        tokens_it = texts
    buf = []
    inserted = 0
    for idx, ((post_id, sent_num, _), tokens) in enumerate(zip(db_cursor, tokens_it)):
        if idx % 10000 == 0:
            if buf:
                vectors, metadata = vectorize_buffer(buf, do_tokenization, model)
                index.add_data(vectors=vectors, metadata=metadata)
                inserted += len(vectors)
                buf = []
            log.debug("Added %s items.." % idx)

        buf.append((tokens, (post_id, sent_num)))

    if buf:
        vectors, metadata = vectorize_buffer(buf, do_tokenization, model)
        index.add_data(vectors=vectors, metadata=metadata)
        inserted += len(vectors)

    log.debug("Buidling index (this may take a long time) ..")
    start = time.time()
    index.build()
    took = time.time() - start
    log.debug("Index build for %s sentences took %s seconds. Now saving index.." % (inserted, took))
    index.save(fname)
    log.debug("Index saved.")


def vectorize_buffer(buf, do_tokenization, model):
    buf_tokens, metadata = zip(*buf)
    vectors = vector_embed_sentence_batch(buf_tokens, tokenized=do_tokenization, model=model)
    vecs_metas = [(vec, meta) for vec, meta in zip(vectors, metadata) if vec is not None]
    vectors, metadata = zip(*vecs_metas)
    return vectors, metadata


def iterate_sents_from_db(count=None, userids=None, dataset="livejournal"):
    tmp_cur = conns[dataset].cursor()
    assert not ((count is not None) and (userids is not None)), "Only specify one filter, userids OR count"
    if count is not None:
        log.debug("Getting %s sents from %s." % (count, dataset))
        tmp_cur.execute(""" SELECT s.post_id, s.sent_num, s.sent_text FROM post_sents s LIMIT %s;""", (count, ))
        filtered = (row for row in tmp_cur if not is_russian(row[-1]))
    elif userids is not None:
        def iter_user_posts():
            for userid in userids:
                tmp_cur.execute(
                    """
                    SELECT
                        s.post_id, s.sent_num, s.sent_text
                    FROM
                        post_sents s,
                        posts p
                     WHERE
                        s.post_id = p.post_id and
                        p.userid = %s;""", (userid, ))
                for row in tmp_cur:
                    if not is_russian(row[-1]):
                        yield row
        filtered = iter_user_posts()
    else:
        found_count = full_db_sent_count()
        log.debug("Getting all %s sents." % found_count)
        tmp_cur.execute(""" SELECT s.post_id, s.sent_num, s.sent_text FROM post_sents s;""", ())
        filtered = (row for row in tmp_cur if not is_russian(row[-1]))

    return filtered


def full_db_sent_count(dataset="livejournal"):
    cur1 = conns[dataset].cursor()
    cur1.execute("""SELECT count(*) from post_sents""")
    found_count = 0
    for row in cur1:
        found_count, = row
    return found_count


# MODELS:
# default
# spacy
# lstm_bc
# lstm_lj
# glove_lj
# glove_bc
if __name__ == '__main__':

    count = 10000000  # How many sentences are we indexing? (None for all sents)
    model_name = "default"  # which vector model are we using? (see above)
    dataset = "reddit"  # what database are we getting sents from? (reddit or livejournal)

    dataset_size_string = "all" if count is None else str(count/1000000)
    fname = "/commuter/inquire_data_root/default/indexes/nms_%sM_%s_%s.idx" % (dataset_size_string, dataset, model_name)

    # fname = "/commuter/inquire_data_root/<vector_train_dataset>/indexes/nms_%s_livejournal_%s.idx" % ("by_userids", model)
    # fname = ""/commuter/inquire_data_root/<vector_train_dataset>/indexes/nms_%s_livejournal_%s.idx" % ("all", model)

    populate_nmslib_index(fname, count=10000000, dataset=dataset)
    log.debug("Wrote saved index to %s" % fname)
