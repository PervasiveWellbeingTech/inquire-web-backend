import pickle

import nltk
import numpy as np


from inquire_sql_backend.semantics.embeddings.vector_models import _get_glove
from python_skipthought_training.training.tools import load_model, encode
import logging
logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s", level=logging.DEBUG)
log = logging.getLogger(__name__)

nltk.data.path.append("/commuter/nltk_data")


class EmbedMap(object):
    def __init__(self, glove_wrapper):
        self.model = glove_wrapper
        self.vocab = self.model.vocab

    def __getitem__(self, item):
        return self.model.__getitem__(item)


# path_to_model = '/commuter/skipthoughts/bookCorpus/model.pkl',
# path_to_dictionary = '/commuter/skipthoughts/bookCorpus/dict'

# SETTINGS FOR BOOKCORPUS
glove_model_name = "glove_bc"
path_to_model = "/commuter/inquire_data_root/bookCorpus/lstm/model.pkl"
path_to_dictionary = "/commuter/inquire_data_root/bookCorpus/lstm/dict"
output_path = '/commuter/inquire_data_root/bookCorpus/lstm/finalized_lstm_glove_bc.pkl'

# SETTINGS FOR LIVEJOURNAL
# glove_model_name = "glove_lj"
# path_to_model = "/commuter/inquire_data_root/livejournal_sample/lstm/model.pkl"
# path_to_dictionary = "/commuter/inquire_data_root/livejournal_sample/lstm/dict"
# output_path = '/commuter/inquire_data_root/livejournal_sample/lstm/finalized_lstm_lj_glove.pkl'


if __name__ == '__main__':
    embed_map = EmbedMap(_get_glove(model_name=glove_model_name))
    model = load_model(embed_map, path_to_model=path_to_model, path_to_dictionary=path_to_dictionary)
    enc = encode(model, ["I hate my commute", "I don't like driving to work"], use_norm=True)
    print(enc)
    print(enc.shape)
    print(float(np.dot(enc[0], enc[1].T)))

    with open(output_path, "wb") as outf:
        pickle.dump(model, outf, protocol=4)
