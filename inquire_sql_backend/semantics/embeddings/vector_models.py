from functools import partial

# from gensim.models.wrappers.fasttext import FastText
import numpy as np
from inquire_sql_backend.semantics.embeddings.glove_wrapper import GloveWrapper
from inquire_sql_backend.semantics.embeddings.util import tokenize, stopwords

import pickle as pkl
import logging

log = logging.getLogger(__name__)

# These should point to the pkl files that were specified as the output file in the "finalize_lstm_model" script
LSTM_PATHS = {
    "lstm_bc": '/commuter/inquire_data_root/bookCorpus/lstm/finalized_lstm_glove_bc.pkl',
    "lstm_lj": "/commuter/inquire_data_root/livejournal_sample/lstm/finalized_lstm_lj_glove.pkl",
}

# These paths should always point at the plain text files that have, on each line, a word followed by its vector
GLOVE_PATHS = {
    "commoncrawl": "/commuter/inquire_data_root/default/model/glove.840B.300d.txt",
    "glove_bc": "/commuter/inquire_data_root/bookCorpus/glove/vectors.txt",
    "glove_lj":  "/commuter/inquire_data_root/livejournal_sample/glove/vectors.txt"
}

# FASTTEXT_PATH = "/commuter/bookCorpus/fasttext/model.300.bin"

_nlp = {}
# _fasttext = None
_glove_wrapped = {}
_lstm_model = {}

_encode = None  # just to make the theano imports optional


def _get_lstm(model_name):
    global _lstm_model
    global _encode
    if model_name not in _lstm_model:
        from python_skipthought_training.training.tools import encode
        _encode = encode
        with open(LSTM_PATHS[model_name], "rb") as inf:
            log.debug("Loading LSTM model from %s" % LSTM_PATHS[model_name])
            model = pkl.load(inf)
            _lstm_model[model_name] = model

    return _lstm_model[model_name]


def _get_glove(model_name):
    global _glove_wrapped
    if _glove_wrapped.get(model_name, None) is None:
        _glove_wrapped[model_name] = GloveWrapper(path=GLOVE_PATHS[model_name])
    return _glove_wrapped[model_name]


# def _get_fasttext():
#     global _fasttext
#     if _fasttext is None:
#         log.debug("Loading fasttext model..")
#         _fasttext = FastText.load_fasttext_format(FASTTEXT_PATH)
#     return _fasttext


def _get_nlp(lang):
    if lang not in _nlp:
        import spacy
        log.debug("Loading %s spacy pipeline.." % lang)
        _nlp[lang] = spacy.load(lang)
    return _nlp[lang]


def vector_embed_sentence_spacy(sentences, batch=False, tokenized=False):
    if not batch:
        sentences = [sentences]
    nlp = _get_nlp("en")

    res = []
    for sent in sentences:
        if tokenized:
            sent = " ".join(sent)  # undo tokenization since spacy does it anyway

        tokens = nlp.tokenize(sent)
        vecs = [word.vector if word.has_vector else None for word in tokens]
        vecs = [v for v in vecs if v is not None]
        if not vecs:
            res.append(None)
        else:
            res.append(np.array(vecs).mean(0))

    if batch:
        return res
    return res[0]


def vector_embed_sentence_glove(sentences, model_name, batch=False, tokenized=False):
    if not batch:
        sentences = [sentences]

    res = []
    for sent in sentences:
        if tokenized:
            tokens = sent
        else:
            tokens = tokenize(sent)

        glove = _get_glove(model_name)
        w_vecs = [glove[word] for word in tokens]
        w_vecs = [v for v in w_vecs if v is not None]
        if not w_vecs:
            res.append(None)
        else:
            sent_vector = np.array(w_vecs).mean(0)
            res.append(sent_vector)
    if batch:
        return res
    return res[0]

warned = False


def vector_embed_sentence_lstm(sentences, model_name, batch=False, tokenized=False):
    global warned
    global _encode

    if not batch:
        sentences = [sentences]

    if tokenized:
        # special case for LSTM: we need to undo the tokenization
        if not warned:
            log.warn("Don't pass tokenized data to LSTM! Has its own tokenization rules.")
            warned = True

        sentences = [" ".join(sent) for sent in sentences]

    lstm = _get_lstm(model_name=model_name)
    res = _encode(lstm, sentences, use_norm=True, batch_size=4096, verbose=False)

    if batch:
        return res
    return res[0]


# def vector_embed_sentence_fasttext(sentence):
#     tokens = tokenize(sentence)
#     ft = _get_fasttext()
#     vecs = []
#     for word in tokens:
#         try:
#             v = ft[word]
#             vecs.append(v)
#         except KeyError:
#             pass
#
#     if not vecs:
#         return None
#     return np.array(vecs).mean(0)


# THIS IS THE CENTRAL LIST OF ALL MODELS

VECTOR_EMBEDDERS = {
    "default": partial(vector_embed_sentence_glove, model_name="commoncrawl"),
    "spacy": vector_embed_sentence_spacy,
    # "fasttext": vector_embed_sentence_fasttext,
    "lstm_bc": partial(vector_embed_sentence_lstm, model_name="lstm_bc"),
    "lstm_lj": partial(vector_embed_sentence_lstm, model_name="lstm_lj"),
    "glove_lj": partial(vector_embed_sentence_glove, model_name="glove_lj"),
    "glove_bc": partial(vector_embed_sentence_glove, model_name="glove_bc"),
}