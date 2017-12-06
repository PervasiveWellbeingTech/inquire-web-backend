import json
import time

from scripts.load_reddit_may15_sql import log

#   CREATE TABLE users (
#   	USERID		INT		PRIMARY KEY,
#   	USERNAME	TEXT	UNIQUE
#   );
#   CREATE TABLE posts (
#       POST_ID     INT     PRIMARY KEY,
#   	EXT_POST_ID	TEXT    NOT NULL,
#   	USERID		INT     NOT NULL,
#   	POST_TIME	INT		NOT NULL,
#   	UNIQUE(EXT_POST_ID, USERID)
#   );
#   CREATE TABLE post_sents (
#   	POST_ID		INT,
#   	SENT_NUM	INT,
#   	SENT_TEXT	TEXT	NOT NULL,
#   	PRIMARY KEY(POST_ID, SENT_NUM)
#   );
#
#   CREATE TABLE posts_misc (
#       POST_ID     INT     PRIMARY KEY,
#       DATA        JSONB
#   )


def write_doc_stream_to_db(inquire_conn, stream, skip_to=None):
    cur = inquire_conn.cursor()
    if skip_to is None:
        skip_to = 0
    inserted_posts = 0
    inserted_sents = 0

    start = time.time()
    for post_id, document in enumerate(stream):
        if post_id % 10000 == 0:
            inquire_conn.commit()
            log.debug(
                "Inserted %s posts in %s seconds far (%s sentences) %s" % (
                    inserted_posts,
                    time.time() - start,
                    inserted_sents,
                    "" if skip_to is None else ("(skipped %s)" % skip_to)
                )
            )

        if post_id < skip_to:
            continue

        insert_post(cur, post_id, document["ext_post_id"], document["userid"], document["post_time"])
        insert_posts_misc(cur, post_id, document["meta"])
        inserted_posts += 1
        for sent_num, sent in enumerate(document["sents"]):
            insert_sent(cur, post_id, sent, sent_num)
            inserted_sents += 1

    inquire_conn.commit()
    log.debug("Done - Inserted %s posts in %s seconds (%s sentences) (~75m posts total%s)" % (
        inserted_posts, time.time() - start, inserted_sents, "" if skip_to is None else (" skipped %s" % skip_to)))


def write_users_to_db(inquire_conn, users_dict):
    cur = inquire_conn.cursor()
    for i, (username, userid) in enumerate(users_dict.items()):
        if i % 1000 == 0:
            inquire_conn.commit()
            log.debug("Inserted %s users.." % i)
        insert_user(cur, userid, username)
    log.debug("Inserted %s users in total." % i)
    inquire_conn.commit()


def insert_sent(cur, post_id, sent, sent_id):
    cur.execute(
        "INSERT INTO post_sents (post_id, sent_num, sent_text) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
        (post_id, sent_id, sent)
    )


def insert_post(cur, post_id, ext_post_id, userid, timestamp):
    cur.execute(
        # TODO this says ext_post_id, which will not work if that field is still called lj_post_id (current prod system)
        # We will need to update that table
        "INSERT INTO posts (post_id, ext_post_id, userid, post_time) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
        (post_id, ext_post_id, userid, timestamp)
    )


def insert_user(cur, userid, username):
    cur.execute("INSERT INTO users (userid, username) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (userid, username))


def insert_posts_misc(cur, post_id, json_data):
    cur.execute(
        "INSERT INTO posts_misc (post_id, data) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
        (post_id, json.dumps(json_data))
    )