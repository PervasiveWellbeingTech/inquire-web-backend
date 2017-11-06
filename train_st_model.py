import theano
from python_skipthought_training.training import vocab, train

theano.config.floatX = 'float32'

fname = "/commuter/inquire_data_root/livejournal_sample/dataset/billion_words.txt"
dict_loc = "/commuter/inquire_data_root/livejournal_sample/lstm/dict"
model_loc = "/commuter/inquire_data_root/livejournal_sample/lstm/model"



# fname = "/commuter/bookCorpus/books_all.txt"
# dict_loc = "/commuter/skipthoughts/bookCorpus/dict"
# model_loc = "/commuter/skipthoughts/bookCorpus/model"


X = []
# /commuter/bookCorpus/books_all.txt is 74 004 228 lines


with open(fname, "r") as inf:
    for line in inf:
        X.append(line.strip())


worddict, wordcount = vocab.build_dictionary(X)
vocab.save_dictionary(worddict, wordcount, dict_loc)

train.trainer(
    X,
    dim_word=300,  # word vector dimensionality
    dim=300,  # the number of GRU units
    encoder='gru',
    decoder='gru',
    max_epochs=5,
    dispFreq=1,
    decay_c=0.,
    grad_clip=5.,
    n_words=20000,
    maxlen_w=30,
    optimizer='adam',
    batch_size=64,
    saveto=model_loc,
    dictionary=dict_loc,
    saveFreq=1000
)