import nmslib
import pickle

from inquire_sql_backend.config import INDEXES_DIRECTORY


class NMSLibIndex(object):
    def __init__(self):
        self.index = nmslib.init(method='hnsw', space='cosinesimil', data_type=nmslib.DataType.DENSE_VECTOR)
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

    def query(self, vector, top=100, **kwargs):
        if "ef" not in kwargs:
            kwargs["ef"] = 2000
        self.index.setQueryTimeParams(kwargs)
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


_indexes = {}

INDEXES = {
    # key format is: (vector_model, dataset_name, fraction_of_data_indexed)
    ("default", "livejournal", 0.01): "nms_10.0M_livejournal_default.idx",
    ("default", "livejournal", 0.1): "nms_100.0M_livejournal_default.idx",
    ("default", "reddit", 0.01): "nms_1.27M_reddit_default.idx",
    ("default", "reddit", 0.1): "nms_10.0M_reddit_default.idx",
    ("default", "reddit", 1.0): "nms_allM_reddit_default.idx",
    ("bert", "livejournal", 0.01): "nms_10.0M_livejournal_bert.idx"
}


def get_nms_index(model="default", dataset="livejournal", percentage=0.01):
    key = (model, dataset, percentage)
    if key not in INDEXES:
        return None
    global _indexes
    if key not in _indexes:
        _indexes[key] = NMSLibIndex.load(INDEXES_DIRECTORY + INDEXES[key])

    return _indexes[key]