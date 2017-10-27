import itertools
import logging
from multiprocessing import pool

import numpy as np
import psycopg2
from annoy import AnnoyIndex
from inquire_sql_backend.embeddings.vectors import vector_embed_sentence

from inquire_sql_backend.semantics.embeddings.util import tokenize

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


def populate_annoy_index(trees=200, debug=False, vector_method="glove"):
    index = AnnoyIndex(300, metric="angular")
    p = pool.Pool(processes=14)

    if debug:
        count = 10000000
    else:
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
        """ + (("\n LIMIT %s;" % (count + 1)) if debug else ";"), ())

    log.debug("Populating index..")

    filtered = (
        row for row in tmp_cur
        if not any([l in row[-1] for l in letters])  # filter out russian posts
    )

    db_cursor, curit2 = itertools.tee(filtered)
    texts = (row[-1] for row in curit2)

    tokens_it = p.imap(tokenize, texts, chunksize=1000)

    with open("/commuter/pdowling/annoy-index/new_%sM_annoy_to_sql_ids_%s_%s.txt" % (count/1000000, vector_method, trees), "w") as outf:
        for id_, ((post_id, sent_num, _), tokens) in enumerate(zip(db_cursor, tokens_it)):
            internal_id = count - id_
            if id_ % 10000 == 0:
                log.debug("Added %s items.." % id_)

            sent_vector = vector_embed_sentence(tokens, method=vector_method)
            if sent_vector is not None:
                index.add_item(internal_id, np.array(sent_vector))
                outf.write("%s\t%s\t%s\n" % (internal_id, post_id, sent_num))

    log.debug("Building annoy index with %s trees.." % trees)
    index.build(trees)
    log.debug("Saving index..")
    index.save("/commuter/pdowling/annoy-index/new_%sM_livejournal_%s_%strees.indx" % (count/1000000, vector_method, trees))
    log.debug("Done!")

if __name__ == '__main__':
    populate_annoy_index(debug=True, trees=200)
