
letters = list("зсьовфдагтурйпб«эыинямжчеклю»ш")


def is_russian(text):
    return any([l in text for l in letters])  # filter out russian posts


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


