import itertools
import logging
from multiprocessing import pool

import psycopg2

from inquire_sql_backend.semantics.embeddings.subspaces import vector_embed_sentence
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

conn = psycopg2.connect("host=localhost dbname=inquire_newparse user=postgres password=12344321")


letters = list("зсьовфдагтурйпб«эыинямжчеклю»ш")


def is_russian(text):
    return any([l in text for l in letters])  # filter out russian posts


def populate_nmslib_index(count=None, vector_method="glove"):
    index = NMSLibIndex()
    p = pool.Pool(processes=14)

    if count is None:
        cur1 = conn.cursor()
        cur1.execute(
            """SELECT count(*) from post_sents"""
        )
        count = 0
        for row in cur1:
            count, = row

    log.debug("Found %s total sents!" % count)
    tmp_cur = conn.cursor()
    tmp_cur.execute(
        """
        SELECT
            s.post_id,
            s.sent_num,
            s.sent_text
        FROM
            post_sents s
        """ + (("\n LIMIT %s;" % (count + 1)) if count is not None else ";"), ())

    log.debug("Populating index..")
    filtered = (row for row in tmp_cur if not is_russian(row[-1]))

    db_cursor, curit2 = itertools.tee(filtered)
    texts = (row[-1] for row in curit2)

    tokens_it = p.imap(tokenize, texts, chunksize=1000)
    buf = []
    for idx, ((post_id, sent_num, _), tokens) in enumerate(zip(db_cursor, tokens_it)):
        if idx % 10000 == 0:
            if buf:
                vectors, metadata = zip(*buf)
                index.add_data(vectors=vectors, metadata=metadata)
                buf = []
            log.debug("Added %s items.." % idx)

        sent_vector = vector_embed_sentence(tokens, method=vector_method)
        if sent_vector is not None:
            buf.append((sent_vector, (post_id, sent_num)))
    if buf:
        vectors, metadata = zip(*buf)
        index.add_data(vectors=vectors, metadata=metadata)

    index.build()
    log.debug("Saving index..")
    fname = "/commuter/inquire-nmslib-index/nms_%sM_livejournal_%s.idx" % (count/1000000, vector_method)
    index.save(fname)
    log.debug("Done!")
    return fname

if __name__ == '__main__':
    wrote_to = populate_nmslib_index(count=10000000)
    log.debug("Wrote saved index to %s" % wrote_to)
