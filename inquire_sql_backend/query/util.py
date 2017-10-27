from flask import request


def apply_filters(nearest_neighbors, max_words, min_words, filter_words, top):
    print("Applying filters..")

    results_to_keep = []

    if (min_words is not None) or (max_words is not None):
        print("enforcing %s <= textlen <= %s" % (min_words, max_words))

    for sent_data in nearest_neighbors:
        if len(results_to_keep) >= top:
            break

        text = sent_data["sent_text"]
        remove = False

        if filter_words is not None:
            for filter_word in filter_words:
                if filter_word in text:
                    remove = True

        if (min_words is not None) or (max_words is not None):
            if not (int(min_words or 0) <= len(text.split()) <= int(max_words or 99999)):
                remove = True

        if not remove:
            results_to_keep.append(sent_data)
    print("Keeping %s candidates." % len(results_to_keep))
    return results_to_keep


def parse_query_params():
    min_words = request.args.get("minWords", None)
    max_words = request.args.get("maxWords", None)
    filter_words = request.args.get("filter", None)
    if filter_words:
        filter_words = [w.strip().lower() for w in filter_words.split(",")]
    top = int(request.args.get("top"))
    top = min(top, 1000)

    any_filters = filter_words is not None or min_words is not None or max_words is not None
    query_string = request.args.get("data")

    return any_filters, filter_words, max_words, min_words, query_string, top