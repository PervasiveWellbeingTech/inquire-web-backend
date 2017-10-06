import numpy as np
import spacy
from sklearn.metrics.pairwise import cosine_similarity
from gensim.models.wrappers.fasttext import FastText
import logging

from inquire_sql_backend.embeddings.glove_wrapper import GloveWrapper
from inquire_sql_backend.embeddings.util import stopwords, tokenize

log = logging.getLogger(__name__)

_nlp = {}
_fasttext = None
_glove_wrapped = None
FASTTEXT_PATH = "/commuter/bookCorpus/fasttext/model.300.bin"
GLOVE_PATH = "/commuter/glove-vectors"


def _get_word_vector(word, method="spacy"):
    if word.lower() in stopwords:
        return None
    if method == "spacy":
        return word.vector if word.has_vector else None
    elif method == "fasttext":
        try:
            return get_fasttext()[str(word)]
        except KeyError:
            return None
    elif method == "glove":
        return get_glove()[str(word)]


def get_glove():
    global _glove_wrapped
    if _glove_wrapped is None:
        _glove_wrapped = GloveWrapper(path=GLOVE_PATH)
    return _glove_wrapped


def get_fasttext():
    global _fasttext
    if _fasttext is None:
        log.debug("Loading fasttext model..")
        _fasttext = FastText.load_fasttext_format(FASTTEXT_PATH)
    return _fasttext


def get_nlp(lang):
    if lang not in _nlp:
        log.debug("Loading %s spacy pipeline.." % lang)
        _nlp[lang] = spacy.load(lang)
    return _nlp[lang]


def vector_embed_sentence(sentence, method="spacy"):
    if type(sentence) == list:
        tokens = sentence
    else:
        tokens = tokenize(sentence)

    vecs = [_get_word_vector(word, method=method) for word in tokens]
    vecs = [v for v in vecs if v is not None]
    if not vecs:
        return None
    return np.array(vecs).mean(0)


def compute_vector_sim(sent1, sent2, method="spacy"):
    a = vector_embed_sentence(sent1, method=method)
    b = vector_embed_sentence(sent2, method=method)
    if a is None or b is None:
        return 0.0
    return cosine_similarity([a], [b])[0][0]