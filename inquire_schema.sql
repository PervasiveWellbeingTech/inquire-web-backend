# DROP TABLE users;
# DROP TABLE posts;
# DROP TABLE post_sents;
# DROP TABLE posts_misc

CREATE TABLE users (
	USERID		INT		PRIMARY KEY,
	USERNAME	TEXT	UNIQUE
);
CREATE TABLE posts (
    POST_ID     INT     PRIMARY KEY,
	EXT_POST_ID	TEXT    NOT NULL,
	USERID		INT     NOT NULL,
	POST_TIME	INT		NOT NULL,
	UNIQUE(EXT_POST_ID, USERID)
);
CREATE TABLE post_sents (
	POST_ID		INT,
	SENT_NUM	INT,
	SENT_TEXT	TEXT	NOT NULL,
	PRIMARY KEY(POST_ID, SENT_NUM)
);

CREATE TABLE posts_misc (
    POST_ID     INT     PRIMARY KEY,
    DATA        JSONB
)
CREATE INDEX post_sents_post_id_only ON post_sents(post_id);
CREATE INDEX posts_userid ON posts(userid);
CREATE INDEX post_sent_ids_bucketed_929 ON post_sents((post_id % 929));
CREATE INDEX post_sent_ids_bucketed_929_mod_and_post_id ON post_sents((post_id % 929), post_id);
