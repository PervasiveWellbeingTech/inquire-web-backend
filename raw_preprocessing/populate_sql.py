import json
import logging
import psycopg2
import time
from dateutil import parser

conn = psycopg2.connect(dbname="inquire_newparse", user="dowling")

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
    level=logging.DEBUG
)
log = logging.getLogger(__name__)
DATA_ROOT = "/commuter/raw_data_livejournal/data"


def run_full():
    start = time.time()
    userid = 0
    num_sents = 0
    last_username = None
    cur = conn.cursor()
    iterator = (
        json.loads(line) for line in open("/commuter/full_lj_parse_philipp/sents/all_posts_sents.json.txt", "r")
    )
    for post_id, doc in enumerate(iterator):
        if post_id % 10000 == 0:
            conn.commit()
            log.debug("Inserted %s posts in %s seconds far (%s sentences, %s users) (~75m posts total)" % (
                post_id, time.time() - start, num_sents, userid))
        username = doc["user"]
        if username != last_username:
            last_username = username
            cur.execute("INSERT INTO users (userid, username) VALUES (%s, %s)", (userid, username))
            userid += 1
        try:
            t = int(parser.parse(doc["eventtime"]).timestamp())
        except ValueError:
            log.debug("couldn't parse time: %s" % doc["eventtime"])
            t = 0
        cur.execute(
            "INSERT INTO posts (post_id, lj_post_id, userid, post_time) VALUES (%s, %s, %s, %s)",
            (post_id, doc["ditemid"], userid, t)
        )
        for sent_id, sent in enumerate(doc["sents"]):
            num_sents += 1
            try:
                cur.execute(
                    "INSERT INTO post_sents (post_id, sent_num, sent_text) VALUES (%s, %s, %s)",
                    (post_id, sent_id, sent)
                )
            except:
                log.exception("Unable to insert sentence %s from post %s! Skipping" % (sent_id, post_id))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    run_full()

