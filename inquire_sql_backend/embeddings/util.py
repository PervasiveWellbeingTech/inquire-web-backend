import string

from nltk import TreebankWordTokenizer

stopwords = {"a", "am", "an", "and", "any", "are", "aren", "as",
             "at", "be", "been", "but", "by", "can",
             "cannot", "could", "couldn", "did", "didn", "do", "does", "doesn", "doing", "don", "down",
             "each", "few", "for", "from", "further", "had", "hadn", "has", "hasn", "have", "haven",
             "having", "he", "her", "here",  "hers", "him",
             "his", "how", "i", "i'd", "i'll", "i'm", "if", "in", "into", "is", "isn't", "it", "it's",
             "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
             "on", "once", "only", "or", "other",  "our", "ours", "out", "over", "own", "same",
             "shan", "she", "she'd", "she'll", "she's", "so", "some", "such", "than", "that",
             "that", "the", "their", "theirs", "them",  "then", "there", "there's", "these", "they",
             "they",  "this", "those",  "to", "up",
             "very", "was", "wasn", "were", "weren", "what", "what",
             "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with",
             "would", "wouldn", "you", "you", "your", "yours", "yourself", "'t", "'s", "'d", "'re", "'ve", "'ll"
             "n't", "!", "...", ",", ".", "?", "-", "``", "''", "{", "}", "[", "]", "(", ")", "’", ":", ";", "--"}

translator = str.maketrans('', '', string.punctuation)


def clean_sent(sent):
    sent = sent.translate(translator).strip().split()
    sent = [word for word in sent if word.lower() not in stopwords]
    return " ".join(sent)


tok = TreebankWordTokenizer()


def tokenize(t):
    return tok.tokenize(clean_sent(t))