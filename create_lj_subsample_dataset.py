import logging
from functools import partial
from multiprocessing import pool
from random import shuffle, seed

import psycopg2

from inquire_sql_backend.query.util import is_russian
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


def sample_journals_from_db(filename, word_count):
    log.debug("Getting usernames..")
    all_users = get_all_users()

    log.debug("Found %s users. " % len(all_users))

    log.debug("Collecting at least %s words worth of journals.." % word_count)
    curr_word_count = 0
    p = pool.Pool(14)
    with open(filename, "w") as outf, open(filename + ".usernames", "w") as outfusers:
        for idx, userid in enumerate(all_users):
            if idx % 100 == 0:
                log.debug(
                    "Got %s users, %s words so far (%s%%)." % (idx, curr_word_count, curr_word_count / word_count * 100)
                )
            tmp_cur = conn.cursor()
            tmp_cur.execute(
                """
                SELECT
                    s.sent_text
                FROM
                    post_sents s,
                    posts p
                WHERE
                    p.userid = %s and
                    s.post_id = p.post_id
                ORDER BY
                    s.post_id asc,
                    s.sent_num asc;
                """, (userid, ))

            filtered = (
                row[0].replace("'", "").replace('"', " `` ", 1).replace('"', " '' ", 1).lower()
                for row in tmp_cur if not is_russian(row[0])
            )
            # db_cursor, curit2 = itertools.tee(filtered)
            # texts = (row[-1] for row in curit2)
            tokens_it = p.imap(partial(tokenize, remove_stop=False), filtered, chunksize=1000)
            outfusers.write("%s\n" % userid)
            for sent in tokens_it:
                curr_word_count += len(sent)
                outf.write(" ".join(sent)+"\n")

            if curr_word_count >= word_count:
                return


def get_all_users():
    seed(123)
    tmp_cur = conn.cursor()
    tmp_cur.execute(
        """
        SELECT
            u.userid
        FROM
            users u;
        """)
    # sort so that the shuffle afterwards is deterministic for our key
    all_users = sorted([int(row[0]) for row in tmp_cur])
    shuffle(all_users)
    return all_users


if __name__ == '__main__':
    fname = "/commuter/inquire_data_root/livejournal_sample/dataset/billion_words.txt"
    sample_journals_from_db(fname, 984845269)  # this is the number of words in bookCorpus
    log.debug("Wrote saved index to %s" % fname)
