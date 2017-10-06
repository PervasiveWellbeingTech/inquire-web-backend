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
while True:
    text = input()
    res = requests.get("http://localhost:9000/query?data=%s&top=20" % text)
    print(json.dumps(res.json(), indent=4))
