import psycopg2
import numpy as np
from annoy import AnnoyIndex
import logging
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

conn = psycopg2.connect("host=localhost dbname=inquire user=postgres password=12344321")


def populate_annoy_index(trees=1000, debug=False):
    index = AnnoyIndex(300, metric="angular")

    if debug:
        count = 1000000
    else:
        cur1 = conn.cursor()
        cur1.execute(
            """SELECT count(*) from post_sents"""
        )
        count = 0
        for row in cur1:
            count, = row

    log.debug("Found %s total sents!" % count)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            s.post_id,
            s.sent_num,
            w.sent_vector
        FROM
            post_sents s, sents_word2vec w
        WHERE
            w.post_id = s.post_id and
            w.sent_num = s.sent_num
        """ + ("\n LIMIT 1000000;" if debug else ";"), ())
    #ordered_res = []
    log.debug("Populating index..")
    with open("/commuter/pdowling/annoy-index/annoy_to_sql_ids_full.txt", "w") as outf:
        for id_, (post_id, sent_num, sent_vector) in enumerate(cur):
            if id_ % 10000 == 0:
                log.debug("Added %s items.." % id_)
            #ordered_res.append((count - id_, post_id, sent_num))
            index.add_item(count - id_, np.array(sent_vector))
            outf.write("%s\t%s\t%s\n" % (count - id_, post_id, sent_num))

    #log.debug("Storing index map..")
    #with open("/commuter/pdowling/annoy-index/annoy_to_sql_ids.txt", "w") as outf:
    #    for tup in ordered_res:

    log.debug("Building annoy index with %s trees.." % trees)
    index.build(trees)
    log.debug("Saving index..")
    index.save("/commuter/pdowling/annoy-index/livejournal_w2v_%strees.indx" % trees)
    log.debug("Done!")

if __name__ == '__main__':
    populate_annoy_index(trees=1000)
