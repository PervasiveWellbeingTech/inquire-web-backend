from operator import itemgetter

import psycopg2

from inquire_sql_backend.config import DB_LIVEJOURNAL_STRING, DB_REDDIT_STRING
import logging
log = logging.getLogger(__name__)

DEC2FLOAT = psycopg2.extensions.new_type(psycopg2.extensions.DECIMAL.values, 'DEC2FLOAT',
                                         lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

conns = {
    "livejournal": psycopg2.connect(DB_LIVEJOURNAL_STRING),
    "reddit": psycopg2.connect(DB_REDDIT_STRING),
    # TODO: your dataset
}


def retrieve_sent_metadata(post_id, sent_num, dataset="livejournal"):
    cur = conns[dataset].cursor()
    log.debug("Getting (%s, %s) from %s" % (post_id, sent_num, dataset))
    if dataset == "livejournal":
        ext_post_id_name = "lj_post_id"  # TODO this is different for Reddit, and will also be adjusted for LJ
    else:
        ext_post_id_name = "ext_post_id"
    cur.execute(
        """
        SELECT
            p.{ext_post_id_name},
            p.post_id,
            s.sent_num,
            s.sent_text,
            u.username,
            p.post_time,
            pm.data
        FROM
            users u, post_sents s,
            posts p left join posts_misc pm on p.post_id = pm.post_id
        WHERE
            s.post_id = p.post_id and
            p.userid = u.userid and
            s.post_id = %s and
            s.sent_num = %s;
        """.format(ext_post_id_name=ext_post_id_name), (post_id, sent_num))
    row = cur.fetchall()[0]
    as_dict = parse_row(dataset, row, fill_url=True)
    # as_dict = dict(zip(["lj_post_id", "post_id", "sent_num", "sent_text", "username", "post_time"], row))
    return as_dict


def retrieve_sent_context_metadata(post_id, sent_num, window=None, dataset="livejournal"):
    cur = conns[dataset].cursor()
    if dataset == "livejournal":
        ext_post_id_name = "lj_post_id"  # TODO this is different for Reddit, and will also be adjusted for LJ
    else:
        ext_post_id_name = "ext_post_id"
    if window is None:
        cur.execute(
            """
            SELECT
                p.{ext_post_id_name},
                p.post_id,
                s.sent_num,
                s.sent_text,
                u.username,
                p.post_time
                pm.data
            FROM
                users u, post_sents s,
                posts p left join posts_misc pm on p.post_id = pm.post_id
            WHERE
                s.post_id = p.post_id and
                p.userid = u.userid and
                s.post_id = %s;
            """.format(ext_post_id_name=ext_post_id_name), (post_id,)
        )
    else:
        cur.execute(
            """
            SELECT
                p.{ext_post_id_name},
                p.post_id,
                s.sent_num,
                s.sent_text,
                u.username,
                p.post_time,
                pm.data
            FROM
                users u, post_sents s,
                posts p left join posts_misc pm on p.post_id = pm.post_id
            WHERE
                s.post_id = p.post_id and
                p.userid = u.userid and
                s.post_id = %s and
                s.sent_num >= %s and
                s.sent_num <= %s;
            """.format(ext_post_id_name=ext_post_id_name), (post_id, sent_num - window, sent_num + window)
        )

    res = cur.fetchall()
    final_dict = None
    sents = []

    for row in res:
        as_dict = parse_row(dataset, row, fill_url=False)

        if as_dict["sent_num"] == sent_num:  # keep the original center of the context
            as_dict = parse_row(dataset, row, fill_url=True)
            final_dict = as_dict

        sents.append((int(as_dict["sent_num"]), as_dict["sent_text"]))
    sents = list(zip(*list(sorted(sents, key=itemgetter(0)))))[1]
    final_dict["context"] = sents
    return final_dict


def retrieve_all_sents(dataset="livejournal", limit=None):
    cur = conns[dataset].cursor("all_sents")
    cur.itersize = 10000
    if limit is None:
        cur.execute(
            """
            SELECT
                ps.post_id,
                ps.sent_num,
                ps.sent_text
            FROM
                post_sents ps
            ORDER BY
                ps.post_id %% 929, ps.post_id;
            """, ()
        )
    else:
        cur.execute(
            """
            SELECT
                ps.post_id,
                ps.sent_num,
                ps.sent_text
            FROM
                post_sents ps
            ORDER BY
                ps.post_id %% 929, ps.post_id
            LIMIT
                %s;
            """, (limit,)
        )
    return cur


def retrieve_user_sents(username, dataset="livejournal"):
    cur = conns[dataset].cursor()
    if dataset == "livejournal":
        ext_post_id_name = "lj_post_id"  # TODO this is different for Reddit, and will also be adjusted for LJ
    else:
        ext_post_id_name = "ext_post_id"
    cur.execute(
        """
        SELECT
            p.{ext_post_id_name},
            p.post_id,
            s.sent_num,
            s.sent_text,
            u.username,
            p.post_time,
            pm.data
        FROM
            users u, post_sents s,
            posts p left join posts_misc pm on p.post_id = pm.post_id
        WHERE
            s.post_id = p.post_id and
            p.userid = u.userid and
            u.username = %s;
        """.format(ext_post_id_name=ext_post_id_name), (username,)
    )
    res = cur.fetchall()
    final = []
    for row in res:
        as_dict = parse_row(dataset, row)

        final.append(as_dict)

    return final


def parse_row(dataset, row, fill_url=True):
    as_dict = dict(zip(["ext_post_id", "post_id", "sent_num", "sent_text", "username", "post_time", "data"], row))

    if not fill_url:
        del as_dict["data"]
        return as_dict

    if dataset == "livejournal":
        as_dict["url"] = "http://%s.livejournal.com/%s.html" % (as_dict["username"], as_dict["ext_post_id"])
    elif dataset == "reddit":
        metadata = as_dict["data"]
        link_id = metadata["link_id"].split("_")[1]
        subreddit = metadata["subreddit"]
        comment_id = as_dict["ext_post_id"]
        as_dict["url"] = "https://www.reddit.com/r/%s/comments/%s/x/%s" % (subreddit, link_id, comment_id)
    else:
        raise ValueError("Unknown dataset")

    # TODO maybe we should just send this too?
    del as_dict["data"]
    return as_dict
