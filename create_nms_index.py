import itertools
import logging
from functools import partial
from multiprocessing import pool, cpu_count

import psycopg2
import time

#from gensim.utils import grouper

def grouper(lst, chunksize):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), chunksize):
        yield lst[i:i + chunksize]



from inquire_sql_backend.config import INDEXES_DIRECTORY
from inquire_sql_backend.query.util import is_russian
from inquire_sql_backend.semantics.embeddings.vector_sim import vector_embed_sentence_batch
from inquire_sql_backend.semantics.embeddings.util import tokenize
from inquire_sql_backend.semantics.nearest_neighbors.NMSLibIndex import NMSLibIndex
from inquire_sql_backend.query.db import conns

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
    level=logging.DEBUG
    )
logging.basicConfig(level=logging.NOTSET)
log = logging.getLogger(__name__)

DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)


def parallel_tokenize(texts_it, p, chunksize=100000):
    buff = []
    for text in texts_it:
        buff.append(text)
        if len(buff) >= chunksize:
            yield from p.map(tokenize, buff)
            buff = []
    yield from p.map(tokenize, buff)


def vectorize_parallel(tokens_buffer, tokenized, model, p):
    batches = grouper(tokens_buffer, chunksize=2000)
    print("Grouper has finished grouping")
    vectors_batched = p.map(
        partial(vector_embed_sentence_batch, tokenized=tokenized, model=model), batches
    )
    for batch in vectors_batched:
        yield from batch


def vectorize_buffer(buf, tokenized, model, p):
    buf_tokens, metadata = zip(*buf)
    # vectors = vector_embed_sentence_batch(buf_tokens, tokenized=do_tokenization, model=model)
    #vectors = vectorize_parallel(buf_tokens, tokenized, model, p)
    vectors = vector_embed_sentence_batch(buf_tokens, tokenized=tokenized, model=model)
    vecs_metas = [(vec, meta) for vec, meta in zip(vectors, metadata) if vec is not None]
    vectors, metadata = zip(*vecs_metas)
    return vectors, metadata


def populate_nmslib_index(fname, count=None, userids=None, model="default", dataset="livejournal", do_tokenization=True):
    index = NMSLibIndex()
    chunksize = 100 #00000

    p = pool.Pool(processes=1)
    p2 = pool.Pool(processes=1)  # this is just to make vectorization a bit faster (each process loads the vector model)

    filtered = iterate_sents_from_db(count=count, userids=userids, dataset=dataset)
    log.debug("Populating index..")

    db_cursor, curit2 = itertools.tee(filtered)
    texts = (row[-1] for row in curit2)

    if do_tokenization:
        # tokens_it = p.imap(tokenize, texts, chunksize=1000)  # TODO this seems to break with server side cursors
        # tokens_it = map(tokenize, texts)
        tokens_it = parallel_tokenize(texts, p, chunksize=chunksize)
    else:
        tokens_it = texts
    buf = []
    inserted = 0
    #a = [x[0] for x in zip(texts)]
    #print(vector_embed_sentence_batch(a, tokenized=False, model=model))
    chunksize = 10000
    for idx, ((post_id, sent_num, sent_original), tokens) in enumerate(zip(db_cursor, tokens_it)):
        #print("%s - %s" % (sent_original, tokens))  # This works
        #print(buf)
        if len(buf) % chunksize == 0 and len(buf):
            #print("In the first loop")
            vectors, metadata = vectorize_buffer(buf, tokenized=do_tokenization, model=model, p=p2)
            print("Added %s items.." % idx)
            index.add_data(vectors=vectors, metadata=metadata)
            inserted += len(vectors)
            buf = []
            log.debug("Added %s items.." % idx)

        buf.append((tokens, (post_id, sent_num)))

    if False:
        print("In the second loop")
        vectors, metadata = vectorize_buffer(buf, do_tokenization, model, p2)
        print("Entered here")
        index.add_data(vectors=vectors, metadata=metadata)
        inserted += len(vectors)

    p.close()
    p2.close()

    log.debug("Buidling index (this may take a long time) ..")
    start = time.time()
    index.build()
    took = time.time() - start
    log.debug("Index build for %s sentences took %s seconds. Now saving index.." % (inserted, took))
    index.save(fname)
    log.debug("Index saved.")

def iterate_sents_from_db(count=None, userids=None, dataset="livejournal"):
    tmp_cur = conns[dataset].cursor("a_tmp_cur")
    # tmp_cur = conns[dataset].cursor()
    assert not ((count is not None) and (userids is not None)), "Only specify one filter, userids OR count"
    if count is not None:
        log.debug("Getting %s sents from %s." % (count, dataset))
        # tmp_cur.execute(
        #     """
        #     SELECT
        #         s.post_id, s.sent_num, s.sent_text
        #     FROM
        #         post_sents s
        #     ORDER BY
        #         s.post_id %% 929, s.post_id
        #     LIMIT %s;""", (count,))
        tmp_cur.execute(
            """
            SELECT
                s.post_id, s.sent_num, s.sent_text
            FROM
                post_sents s
            LIMIT %s;""", (count,))
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
                        p.userid = %s
                    ORDER BY
                        p.post_id %% 929, p.post_id;
                    """, (userid, ))
                for row in tmp_cur:
                    if not is_russian(row[-1]):
                        yield row
        filtered = iter_user_posts()
    else:
        found_count = full_db_sent_count(dataset=dataset)
        log.debug("Getting all %s sents." % found_count)
        tmp_cur.execute("""
            SELECT
                s.post_id, s.sent_num, s.sent_text
            FROM
                post_sents s;
                """, ())
        filtered = (row for row in tmp_cur if not is_russian(row[-1]))

    return filtered


def full_db_sent_count(dataset="livejournal"):
    cur1 = conns[dataset].cursor()
    cur1.execute("""SELECT count(*) from post_sents""")
    found_count = 0
    for row in cur1:
        found_count, = row
    return found_count


# AVAILABLE MODELS:
# default  # GloVe vectors trained on 840B tokens of commoncrawl data. Usually you'll want to use this!
# spacy  # SpaCy GloVe vectors. Not used in production but can be useful for local development.
# lstm_bc  # _bc means BookCorpus, _lj is trained on a ~1B word sample of livejournal
# lstm_lj
# glove_lj
# glove_bc

if __name__ == '__main__':
    # ## STEP 1: SPECIFY COUNT - You'll usually want to create indexes of 1%, 10% and 100% of your data, as long as you
    # do not index more than 100 million sentences (the index will become too large at that point.

    #count = 100000000  # How many sentences are we indexing? (None for all sents)
    #count = 10000000
    count = 10000000
    # count = 1270000
    # count = None  # A count of "None" will select all of your sentences. Don't sure if you have a very large dataset.

    # ## STEP 2: SELECT VECTOR MODEL ##
    model_name = "bert"  # which vector model are we using? (see above for choices) Usually, leave it as "default"

    # ## STEP 3: SELECT A DATABASE ##
    # what database are we getting sents from? (reddit, livejournal, your custom dataset name)
    #dataset = "livejournal"  # must match a key specified in inquire_sql_backend.query.db.conns
    dataset = "reddit"

    # ## DON'T CONFIGURE BELOW THIS LINE ##
    dataset_size_string = "all" if count is None else str(count/1000000)
    index_fname = "nms_%sM_%s_%s.idx" % (dataset_size_string, dataset, model_name)
    fname = INDEXES_DIRECTORY + index_fname

    populate_nmslib_index(fname, count=count, dataset=dataset,do_tokenization=False,model=model_name)
    log.debug("Index written to %s" % fname)
    log.debug("Index file name is: '%s'" % index_fname)
    log.debug("Add this file name to 'inquire_sql_backend.semantics.nearest_neighbors.NMSLibIndex.INDEXES' to use it.")