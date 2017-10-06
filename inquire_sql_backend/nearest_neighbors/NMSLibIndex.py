import nmslib
import pickle


class NMSLibIndex(object):
    def __init__(self):
        self.index = nmslib.init()
        self._id_counter = 0
        self._metadata = {}

    def add_data(self, vectors, metadata):
        num_vecs = len(vectors)
        assert len(metadata) == num_vecs, "metadata and vectors do not align"

        ids = range(self._id_counter, self._id_counter + num_vecs)
        self._id_counter += num_vecs

        for _id, metadatum in zip(ids, metadata):
            self._metadata[_id] = metadatum

        self.index.addDataPointBatch(data=vectors, ids=ids)

    def query(self, vector, top=100):
        self.index.setQueryTimeParams({
            "ef": 2000,
        })
        ids, dists = self.index.knnQuery(vector, k=top)
        metadata = [self._metadata[_id] for _id in ids]
        return metadata, dists

    def build(self, M=32, efConstruction=1000, maxM=64, maxM0=64):
        self.index.createIndex({
            "M": M,
            "efConstruction": efConstruction,
            "maxM": maxM,
            "maxM0": maxM0,
            "delaunay_type": 2
        }, print_progress=True)

    def save(self, fname):
        self.index.saveIndex(fname)
        with open(fname + ".state", "wb") as outf:
            state = (self._id_counter, self._metadata)
            pickle.dump(state, outf)

    @classmethod
    def load(cls, fname):
        self = cls()
        self.index.loadIndex(fname, True)
        with open(fname + ".state", "rb") as inf:
            state = pickle.load(inf)
            self._id_counter, self._metadata = state
        return self
