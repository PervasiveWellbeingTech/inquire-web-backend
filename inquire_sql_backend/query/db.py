from operator import itemgetter

import psycopg2

DEC2FLOAT = psycopg2.extensions.new_type(psycopg2.extensions.DECIMAL.values, 'DEC2FLOAT',
                                         lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)
conn = psycopg2.connect("host=localhost dbname=inquire_newparse user=postgres password=12344321")


def retrieve_sent_metadata_lj(post_id, sent_num):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.lj_post_id,
            p.post_id,
            s.sent_num,
            s.sent_text,
            u.username,
            p.post_time,
            p.mood
        FROM
            users u, posts p, post_sents s
        WHERE
            s.post_id = p.post_id and
            p.userid = u.userid and
            s.post_id = %s and
            s.sent_num = %s;
        """, (post_id, sent_num))
    res = cur.fetchall()[0]
    as_dict = dict(zip(["lj_post_id", "post_id", "sent_num", "sent_text", "username", "post_time", "mood"], res))
    as_dict["url"] = "http://%s.livejournal.com/%s.html" % (as_dict["username"], as_dict["lj_post_id"])
    return as_dict


def retrieve_sent_context_metadata_lj(post_id, sent_num, window=None):
    cur = conn.cursor()
    if window is None:
        cur.execute(
            """
            SELECT
                p.lj_post_id,
                p.post_id,
                s.sent_num,
                s.sent_text,
                u.username,
                p.post_time,
                p.mood
            FROM
                users u, posts p, post_sents s
            WHERE
                s.post_id = p.post_id and
                p.userid = u.userid and
                s.post_id = %s;
            """, (post_id,)
        )
    else:
        cur.execute(
            """
            SELECT
                p.lj_post_id,
                p.post_id,
                s.sent_num,
                s.sent_text,
                u.username,
                p.post_time,
                p.mood
            FROM
                users u, posts p, post_sents s
            WHERE
                s.post_id = p.post_id and
                p.userid = u.userid and
                s.post_id = %s and
                s.sent_num >= %s and
                s.sent_num <= %s;
            """, (post_id, sent_num - window, sent_num + window)
        )

    res = cur.fetchall()
    final_dict = None
    sents = []
    for row in res:
        as_dict = dict(zip(["lj_post_id", "post_id", "sent_num", "sent_text", "username", "post_time", "mood"], row))
        if as_dict["sent_num"] == sent_num:  # keep the original center of the context
            final_dict = as_dict
            final_dict["url"] = "http://%s.livejournal.com/%s.html" % (as_dict["username"], as_dict["lj_post_id"])

        sents.append((int(as_dict["sent_num"]), as_dict["sent_text"]))
    sents = list(zip(*list(sorted(sents, key=itemgetter(0)))))[1]
    final_dict["context"] = sents
    return final_dict


def retrieve_user_sents(username):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.lj_post_id,
            p.post_id,
            s.sent_num,
            s.sent_text,
            u.username,
            p.post_time,
            p.mood
        FROM
            users u, posts p, post_sents s
        WHERE
            s.post_id = p.post_id and
            p.userid = u.userid and
            u.username = %s;
        """, (username,)
    )
    res = cur.fetchall()
    final = []
    for row in res:
        as_dict = dict(zip(["lj_post_id", "post_id", "sent_num", "sent_text", "username", "post_time", "mood"], row))
        as_dict["url"] = "http://%s.livejournal.com/%s.html" % (username, as_dict["lj_post_id"])
        final.append(as_dict)

    return final
