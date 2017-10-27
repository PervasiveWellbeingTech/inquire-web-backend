import logging
from collections import defaultdict

import numpy as np
import spacy
from gensim.models.wrappers.fasttext import FastText
from inquire_sql_backend.embeddings.glove_wrapper import GloveWrapper
from scipy import linalg
from sklearn.decomposition.pca import _infer_dimension_
from sklearn.utils.extmath import svd_flip

from inquire_sql_backend.semantics.embeddings.util import stopwords

log = logging.getLogger(__name__)

_nlp = {}
_fasttext = None
_glove_wrapped = None
FASTTEXT_PATH = "/commuter/bookCorpus/fasttext/model.300.bin"


def _get_word_vector(word, method="spacy"):
    if word in stopwords:
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
        _glove_wrapped = GloveWrapper()
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


def vector_embed_sentence(sentence, lang="en", method="spacy", normalize=False):
    # nlp = get_nlp(lang)
    vecs = [_get_word_vector(word, method=method) for word in sentence.split(" ")]
    vecs = [v for v in vecs if v is not None]
    if not vecs:
        return None
    r = np.array(vecs).mean(0)
    if normalize:
        return r / np.linalg.norm(r)

    return r


def compute_vector_sim(sent1, sent2, normalized=False, lang="en", method="spacy", _embed=True):
    if _embed:
        sent1 = vector_embed_sentence(sent1, lang=lang, method=method)
        sent2 = vector_embed_sentence(sent2, lang=lang, method=method)
    if sent1 is None or sent2 is None:
        return 0.0
    if not normalized:
        sent1 /= np.linalg.norm(sent1)
        sent2 /= np.linalg.norm(sent1)
    return float(np.dot(sent1, sent2.T))



def get_pca_components(X, n_components):
    """Same as in sklearn, but we don't center the data"""
    n_samples, n_features = X.shape
    U, S, V = linalg.svd(X, full_matrices=False)
    # flip eigenvectors' sign to enforce deterministic output
    U, V = svd_flip(U, V)

    components_ = V

    # Get variance explained by singular values
    explained_variance_ = (S ** 2) / n_samples
    total_var = explained_variance_.sum()
    explained_variance_ratio_ = explained_variance_ / total_var

    # Postprocess the number of components required
    if n_components == 'mle':
        n_components = _infer_dimension_(explained_variance_, n_samples, n_features)
    elif 0 < n_components < 1.0:
        # number of components for which the cumulated explained
        # variance percentage is superior to the desired threshold
        ratio_cumsum = explained_variance_ratio_.cumsum()
        n_components = np.searchsorted(ratio_cumsum, n_components) + 1

    # Compute noise covariance using Probabilistic PCA model
    # The sigma2 maximum likelihood (cf. eq. 12.46)
    # if n_components < min(n_features, n_samples):
    #     noise_variance_ = explained_variance_[n_components:].mean()
    # else:
    #     noise_variance_ = 0.

    components_ = components_[:n_components]
    # explained_variance_ = explained_variance_[:n_components]
    # explained_variance_ratio_ = explained_variance_ratio_[:n_components]

    return components_

dyn_embedding_sizes = defaultdict(lambda: defaultdict(int))
dyn_embedding_len_sizes = defaultdict(lambda: [[0]*300]*300)


def subspace_embed_sentence(sentence, rank_or_energy=0.9, lang="en", method="spacy"):
    # nlp = get_nlp(lang)
    sentence = str(sentence)
    vecs = [_get_word_vector(word, method=method) for word in sentence.split()]
    vecs = [v for v in vecs if v is not None]

    if len(vecs) == 0:
        return None

    components = get_pca_components(np.array(vecs), rank_or_energy)
    # if rank_or_energy <= 1.0:
    #     dyn_embedding_sizes[rank_or_energy][len(components)] += 1
    #     dyn_embedding_len_sizes[rank_or_energy][len(vecs) - 1][len(components) - 1] += 1
    # pca = PCA(svd_solver="full", n_components=rank_or_energy)
    # pca.fit(np.array(vecs))
    return components


def subspace_similarity(subspace_1, subspace_2):
    rank = min(len(subspace_1), len(subspace_2))
    prod = np.dot(subspace_1, subspace_2.T)
    sim = np.linalg.norm(prod) / np.sqrt(rank)
    # u, s, v = svd(prod)
    # sim = np.sqrt((s**2).sum()) / np.sqrt(len(s))
    return sim


def compute_subspace_sim(sent1, sent2, rank_or_energy=0.9, lang="en", method="spacy"):
    s1 = subspace_embed_sentence(sent1, rank_or_energy=rank_or_energy, lang=lang, method=method)
    s2 = subspace_embed_sentence(sent2, rank_or_energy=rank_or_energy, lang=lang, method=method)
    if s1 is None:
        log.warn("no embeddings for '%s'" % sent1)
        return 0.0
    if s2 is None:
        log.warn("no embeddings for '%s'" % sent2)
        return 0.0
    return subspace_similarity(s1, s2)
