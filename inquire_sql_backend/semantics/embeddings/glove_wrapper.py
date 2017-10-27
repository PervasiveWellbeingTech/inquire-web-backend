import os
import numpy as np
import logging
import pickle
log = logging.getLogger(__name__)


class GloveWrapper(object):
    def __init__(self, path="/commuter/glove-vectors"):
        self.path = path
        vector_file = os.path.join(path, "glove.840B.300d.txt")
        self.vocab = {}

        log.debug("Reading vocab..")

        binpath = os.path.join(self.path, "saved.pkl")
        if os.path.exists(binpath):
            with open(binpath, "rb") as inf:
                self.vectors, self.vocab = pickle.load(inf)
        else:
            self.vectors = []

            with open(vector_file, "r") as inf:
                i = 0
                for li, line in enumerate(inf):
                    word, raw = line.strip().split(" ", maxsplit=1)
                    raw = raw.split(" ")
                    if len(raw) != 300:
                        log.error("Couldn't read vector on line %s." % i)
                        continue

                    self.vocab[word] = i
                    vector = np.array([float(n) for n in raw])
                    self.vectors.append(vector)
                    i += 1
            log.debug("Read %s entries (%s lines processed)" % (len(self.vocab), li + 1))
            self.vectors = np.vstack(self.vectors)
            with open(binpath, "wb") as outf:
                pickle.dump((self.vectors, self.vocab), outf, protocol=4)

    def __getitem__(self, word):
        id = self.vocab.get(word, None)
        if id is not None:
            return self.vectors[id]
        return None  # self.vectors[-1]  # UNK token
