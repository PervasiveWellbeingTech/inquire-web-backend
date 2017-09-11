# this creates a simple plain text format that we can use for training vector models
import time
import json
start = time.time()
with open("/commuter/full_lj_parse_philipp/sents/all_posts_sents.json.txt", "r") as inf:
    with open("/commuter/full_lj_parse_philipp/sents/just_sents.lower.txt", "w") as l_outf:
        with open("/commuter/full_lj_parse_philipp/sents/just_sents.txt", "w") as outf:
            for i, line in enumerate(inf):
                if i % 100000 == 0:
                    print("Processed %s lines in %s seconds" % (i, time.time() - start))
                for sent in json.loads(line[:-1])["sents"]:
                    outf.write(sent)
                    outf.write("\n")
                    l_outf.write(sent.lower())
                    l_outf.write("\n")