from inquire_sql_backend.semantics.embeddings.vector_models import VECTOR_EMBEDDERS


def vector_embed_sentence(sent, tokenized=False, model="default"):
    embed_func = VECTOR_EMBEDDERS[model]
    return embed_func(sent, batch=False, tokenized=tokenized)


def vector_embed_sentence_batch(sent, tokenized=False, model="default"):
    embed_func = VECTOR_EMBEDDERS[model]
    return embed_func(sent, batch=True, tokenized=tokenized)