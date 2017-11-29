import itertools
import spacy
import json
import logging
import multiprocessing
n_threads = int(multiprocessing.cpu_count() / 2.)
import sys

logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
        level=logging.DEBUG
    )
log = logging.getLogger(__name__)

nlp = spacy.load("en", entity=False)
DATA_ROOT = "/commuter/raw_data_livejournal/data"

CHUNKSIZE = 50000


def handle_documents(document_dict_iterator, sent_counts=False):
    """
    Handles a stream of documents (dicts) and sentence tokenize the "text" field using spacy.
    """
    for_text_stream, doc_stream = itertools.tee(document_dict_iterator)
    text_stream = (" ".join(d["text"].split()) for d in for_text_stream)
    for i, (doc, processed) in enumerate(
            zip(doc_stream, nlp.pipe(text_stream, batch_size=CHUNKSIZE, n_threads=n_threads - 1))
    ):
        if i % 10000 == 0:
            log.debug("Processed %s documents.." % i)
        del doc["text"]
        doc["sents"] = [str(sent) for sent in processed.sents]
        if sent_counts:
            yield doc, len(doc["sents"])
        else:
            yield doc


def run_full(input_file):
    count = 0
    with multiprocessing.Pool(processes=n_threads - 1) as pool:
        with open("/commuter/full_lj_parse_philipp/" + input_file, "r") as inf:
            # iterator = pool.imap_unordered(
            #     json.loads, inf,
            #     chunksize=CHUNKSIZE
            # )
            iterator = map(json.loads, inf)
            with open("/commuter/full_lj_parse_philipp/sents/" + input_file, "w") as outf:
                for doc, c in handle_documents(iterator, sent_counts=True):
                    outf.write(json.dumps(doc))
                    outf.write("\n")
                    count += c
        log.debug("Total sent count: %s" % count)

if __name__ == '__main__':


    run_full(sys.argv[1])

