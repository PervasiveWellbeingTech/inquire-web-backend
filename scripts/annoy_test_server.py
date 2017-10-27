import requests
import json
#@app.route("/query")
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
while True:
    inp = input("Enter 1 query, 2 to get contexts, 3 to query a user, 4 to get a user's posts")
    if inp == "1":
        text = input("Query: ")
        res = requests.get(url + "/query?data=%s&top=20" % text)
        parsed = res.json()
        print(json.dumps(parsed, indent=4))
    if inp == "2":
        window = int(input("Window size: "))
        res2 = requests.post(url + "/contexts", json={
            "window": window,
            "sents": [(s["post_id"], s["sent_num"]) for s in parsed["query_results"]]
        })
        parsed2 = res2.json()
        print(json.dumps(parsed2, indent=4))
    if inp == "3":
        username = input("Enter username")
        text = input("Query: ")
        res2 = requests.get(url + "/user_query/%s?data=%s&top=20" % (username, text))
        parsed2 = res2.json()
        print(json.dumps(parsed2, indent=4))
    if inp == "4":
        username = input("Enter username")
        res2 = requests.get(url + "/user/%s" % username)
        parsed2 = res2.json()
        print(json.dumps(parsed2, indent=4))




