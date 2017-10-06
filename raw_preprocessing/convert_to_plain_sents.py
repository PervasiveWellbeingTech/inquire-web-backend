# this creates a simple plain text format that we can use for training vector models
import time
import json
import spacy
from multiprocessing import pool
from nltk.tokenize.treebank import TreebankWordTokenizer
# nlp = spacy.load("en")
start = time.time()

p = pool.Pool(processes=14)
tok = TreebankWordTokenizer()

def get_sents():
    with open("/commuter/full_lj_parse_philipp/sents/all_posts_sents.json.txt", "r") as inf:
        for i, line in enumerate(inf):
            if i % 100000 == 0:
                print("Processed %s lines in %s seconds" % (i, time.time() - start))
            for sent in json.loads(line[:-1])["sents"]:
                yield sent


with open("/commuter/full_lj_parse_philipp/sents/just_sents.lower.txt", "w") as l_outf:
    with open("/commuter/full_lj_parse_philipp/sents/just_sents.txt", "w") as outf:
        for sent in p.imap(tok.tokenize, get_sents(), chunksize=100000):
            sent = " ".join([str(w) for w in sent])
            outf.write(sent)
            outf.write("\n")
            l_outf.write(sent.lower())
            l_outf.write("\n")
