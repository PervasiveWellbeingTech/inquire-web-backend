from inquire_sql_backend.query.db import conns
from raw_preprocessing.sentence_tokenize import handle_documents
from raw_preprocessing.populate_sql import insert_post, insert_sent, insert_user, insert_posts_misc
import sqlite3
import logging
import time
log = logging.getLogger(__name__)

reddit_conn = sqlite3.connect("/commuter/mallickd/database.sqlite")
inquire_conn = conns["reddit"]


reddit_col_names = [
    "created_utc",
    "ups",
    "subreddit_id",
    "link_id",
    "name",
    "score_hidden",
    "author_flair_css_class",
    "author_flair_text",
    "subreddit",
    "id",
    "removal_reason",
    "gilded",
    "downs",
    "archived",
    "author",
    "score",
    "retrieved_on",
    "body",
    "distinguished",
    "edited",
    "controversiality",
    "parent_id",
]

meta_cols_only = [
    "ups",
    "link_id",
    "name",
    "score_hidden",
    "author_flair_text",
    "subreddit",
    "removal_reason",
    "gilded",
    "downs",
    "archived",
    "score",
    "distinguished",
    "edited",
    "controversiality",
    "parent_id"
]
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


def stream_reddit_as_docs(users_dict):
    post_id = 0
    res = reddit_conn.execute("SELECT * FROM May2015;")

    for row in res:
        as_dict = dict(zip(reddit_col_names, row))
        username = as_dict["author"]
        userid = users_dict.setdefault(username, len(users_dict))
        as_dict_translated = {
            "post_id": post_id,
            "userid": userid,
            "ext_post_id": as_dict["id"],
            "post_time": as_dict["created_utc"],
            "meta": {key: as_dict[key] for key in meta_cols_only},
            "text": as_dict["body"]
        }
        post_id += 1
        yield as_dict_translated


def write_doc_stream_to_db(stream, skip_to=None):
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


def write_users_to_db(users_dict):
    cur = inquire_conn.cursor()
    for i, (username, userid) in enumerate(users_dict.items()):
        if i % 1000 == 0:
            inquire_conn.commit()
            log.debug("Inserted %s users.." % i)
        insert_user(cur, userid, username)
    log.debug("Inserted %s users in total." % i)
    inquire_conn.commit()


def run_full_import():
    users_seen = dict()
    sent_tokenized_reddit_stream = handle_documents(stream_reddit_as_docs(users_seen))
    write_doc_stream_to_db(sent_tokenized_reddit_stream)
    write_users_to_db(users_seen)
