import logging
import sqlite3

from inquire_sql_backend.data_import import write_doc_stream_to_db, write_users_to_db
from inquire_sql_backend.query.db import conns
from raw_lj_preprocessing.sentence_tokenize import sent_tokenize_documents

log = logging.getLogger(__name__)


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


def stream_reddit_as_docs(users_dict):
    # This is the main reddit wrapper, which you should roughly imitate when you implement a new data source
    # Look at the dict/json format we generate here - one "post" (not necessarily one sentence) from your data source
    # should be represented in this format.
    #
    # Output from this method will be passed to the "sent_tokenize_documents" method
    #
    # This "users_dict" parameter is a dictionary that will be updated to track username -> user_id mappings
    # If users do not occur in an ordered way in your dataset, this type of approach is necessary to keep track of new
    # and existing users.

    reddit_conn = sqlite3.connect("/commuter/mallickd/database.sqlite")  # SQLite to read the raw Reddit input

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


def run_full_import():
    # The Postgres database we will import the data into -> make sure you create and choose a new database if you're
    # importing a new dataset.
    inquire_conn = conns["reddit"]
    users_seen = dict()

    # Replace this with your custom stream
    document_stream = stream_reddit_as_docs(users_seen)

    sent_tokenized_reddit_stream = sent_tokenize_documents(document_stream)
    write_doc_stream_to_db(inquire_conn, sent_tokenized_reddit_stream)
    write_users_to_db(inquire_conn, users_seen)
