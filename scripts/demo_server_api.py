import requests
import json

# @app.route("/query")
# ef query():
#    print("args: %s" % request.args)
#    min_words = request.args.get("minWords", None)
#    max_words = request.args.get("maxWords", None)
#    # nns = request.args.get("nns", 100)

#    filter_words = request.args.get("filter", None)
#    if filter_words:
#        filter_words = [w.strip() for w in filter_words.split(",")]
#    top = int(request.args.get("top"))

#    search_k = request.args.get("search_k", 1000000)

#    any_filters = filter_words is not None or min_words is not None or max_words is not None
#    query_string = request.args.get("data")
url = "http://commuter.stanford.edu:9001"
# url = "http://madmax5.stanford.edu:9001"
# url = "http://commuter.stanford.edu:9001"


# dataset = "reddit"
dataset = "livejournal"

percentage = 0.01  # default for LJ
# percentage = 0.1
top = 5

model = "default"
# model = "lstm_bc"
# model = "lstm_lj"
# model = "glove_bc"
# model = "glove_lj"

while True:
    inp = input(
        "Enter 1 to query, 2 to get contexts, 3 to query a user, 4 to get a user's posts, 5 for a brute force query (takes a long time!), 6 to switch DBs (current is %s)"
        % dataset
    )

    if inp == "1":
        text = input("Query: ")
        res = requests.get(
            url + "/query?data=%s&top=%s&dataset=%s&model=%s&percentage=%s" % (text,top, dataset, model, percentage)
        )
        parsed = res.json()
        print(json.dumps(parsed, indent=4))

    if inp == "2":
        window = int(input("Window size: "))
        res2 = requests.post(url + "/contexts", json={
            "window": window,
            "sents": [(s["post_id"], s["sent_num"]) for s in parsed["query_results"]],
            "dataset": dataset
        })
        parsed2 = res2.json()
        print(json.dumps(parsed2, indent=4))

    if inp == "3":
        username = input("Enter username")
        text = input("Query: ")
        res2 = requests.get(
            url + "/user_query/%s?data=%s&top=20&dataset=%s&model=%s"
            % (username, text, dataset, model)
        )
        parsed2 = res2.json()
        print(json.dumps(parsed2, indent=4))

    if inp == "4":
        username = input("Enter username")
        res2 = requests.get(url + "/user/%s?&dataset=%s" % (username, dataset))
        parsed2 = res2.json()
        print(json.dumps(parsed2, indent=4))

    if inp == "5":
        inp = None
        queries = []
        while True:
            inp = input("Query: ")
            if inp == "":
                break
            queries.append(inp)

        res = requests.get(
            url + "/query_bruteforce?&dataset=%s&%s&top=%s&model=%s&percentage=%s"
            % (dataset, "&".join(["data=%s" % t for t in queries]), top,  model, 100000)
        )
        parsed = res.json()
        print(json.dumps(parsed, indent=4))

    if inp == "6":
        dataset = ("reddit" if dataset == "livejournal" else "livejournal")
        print("DB is now %s" % dataset)
