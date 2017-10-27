import numpy as np
import pickle as pkl
from python_skipthought_training.training.tools import encode


_lstm_model = None
def load_lstm(method="glove"):
    global _lstm_model
    if _lstm_model is None:
        path = '/commuter/skipthoughts/bookCorpus/finalized_%s_lstm.pkl' % method
        with open(path, "rb") as inf:
            model = pkl.load(inf)
        _lstm_model = model
    return _lstm_model

while True:
    sent1 = input("Enter sent 1: ")
    sent2 = input("Enter sent 2: ")
    enc = encode(model, [sent1, sent2], use_norm=True)
    print("Similarity: %s" % np.dot(enc[0], enc[1].T))