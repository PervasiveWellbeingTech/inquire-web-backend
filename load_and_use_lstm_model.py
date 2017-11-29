"""
One-off script just to load an LSTM model and how to use it. This is not super important code.
"""
import numpy as np
import pickle as pkl
from python_skipthought_training.training.tools import encode

path = '/commuter/skipthoughts/bookCorpus/finalized_glove_lstm.pkl'
with open(path, "rb") as inf:
    model = pkl.load(inf)


while True:
    sent1 = input("Enter sent 1: ")
    sent2 = input("Enter sent 2: ")
    enc = encode(model, [sent1, sent2], use_norm=True)
    print("Similarity: %s" % np.dot(enc[0], enc[1].T))