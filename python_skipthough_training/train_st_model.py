import theano
from python_skipthough_training.training import vocab, train

theano.config.floatX = 'float32'
X = []
with open("/commuter/bookCorpus/books_all.txt", "r") as inf:
    for line in inf:
        X.append(line.strip())

dict_loc = "/commuter/skipthoughts/bookCorpus/dict"
model_loc = "/commuter/skipthoughts/bookCorpus/model"

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