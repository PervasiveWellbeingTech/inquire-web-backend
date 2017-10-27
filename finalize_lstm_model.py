import pickle

import nltk
import numpy as np


from inquire_sql_backend.semantics.embeddings.vectors import get_glove, get_fasttext
from python_skipthought_training.training.tools import load_model, encode

nltk.data.path.append("/commuter/nltk_data")


class EmbedMap(object):
    def __init__(self, method="glove"):
        if method == "glove":
            self.model = get_glove()
            self.vocab = self.model.vocab
        elif method == "fasttext":
            self.model = get_fasttext()
            self.vocab = self.model.wv.vocab

    def __getitem__(self, item):
        return self.model.__getitem__(item)

if __name__ == '__main__':
    method = "glove"
    embed_map = EmbedMap(method=method)
    model = load_model(embed_map)
    enc = encode(model, ["I hate my commute", "I don't like driving to work"], use_norm=True)
    print(enc)
    print(enc.shape)
    print(float(np.dot(enc[0], enc[1].T)))
    print(float(np.dot(enc[0], enc[1].T) / (np.linalg.norm(enc[0] * np.linalg.norm(enc[1])))))
    with open('/commuter/skipthoughts/bookCorpus/finalized_%s_lstm.pkl' % method, "wb") as outf:
        pickle.dump(model, outf, protocol=4)
