from flask import Flask, jsonify, request
from pymongo import MongoClient
from nltk.corpus import stopwords

app = Flask(__name__)
app.config["DEBUG"] = True

client = MongoClient("mongodb://localhost:27017")
db = client.FinalDB
tasks_collection = db.Tweets


@app.route('/api/query1', methods=['GET'])
def query1():
    """
    Query to find overall number of tweets on coronavirus.

    :return: A json representation of array of documents(ans) which is passed an argument.
    """
    ans = tasks_collection.count()
    return jsonify(total_count=ans)


@app.route('/api/query2', methods=['GET'])
def query2():
    """
    Query to find overall number of tweets on coronavirus per country.

    :return: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.aggregate([
        {'$group':
             {'_id': '$location',
              'count': {'$sum': 1}
              }
         },
        {'$sort': {'count': -1}}
    ])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


@app.route('/api/query3', methods=['GET'])
def query3():
    """
    Query to find overall number of tweets per country on a daily basis.

    :return: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.aggregate([
        {'$group':
             {'_id': {'location': '$location', 'date': '$date'},
              'count': {'$sum': 1}
              }
         },
        {'$sort': {'count': -1}}
    ])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


@app.route('/api/query4', methods=['GET'])
def query4():
    """
    Query to find the top 100 words occurring on tweets involving coronavirus. (words should be nouns/verbs and
    not involving common ones like the, is, are, etc etc).

    :return: A json representation of array of documents(ans) which is passed an argument.
    """
    en_stops = list(set(stopwords.words('english')))
    cursor = tasks_collection.aggregate([
        {'$project': {'word': {'$split': ["$tweet", " "]}}},
        {'$unwind': "$word"},
        {'$project': {'word': {'$toLower': "$word"}}},
        {'$match': {
            '$and': [
                {'word': {'$ne': ""}},
                {'word': {'$nin': en_stops}}
            ]}},
        {'$group':
             {'_id': {'word': '$word'},
              'count': {'$sum': 1}
              }
         },
        {'$sort': {'count': -1}},
        # {'limit':100}
        {'$limit': 100}
    ])

    # ans = []
    # for doc in cursor:
    #     if doc.word not in en_stops:
    #         ans.append(doc)

    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


# @app.route('/api/query5', methods=['GET'])
# def query5():
#     en_stops = list(set(stopwords.words('english')))
#     cursor = tasks_collection.aggregate([
#         {'$project': {'location': 1, 'word': {'$split': ["$tweet", " "]}}},
#         {'$unwind': "$word"},
#         {'$project': {'word': {'$toLower': "$word"}, 'location': 1}},
#         {'$match': {
#             '$and': [
#                 {'word': {'$ne': ""}},
#                 {'word': {'$nin': en_stops}}
#             ]}},
#         {'$group':
#              {'_id': {'location': '$location', 'word': '$word'},
#               # {'_id': {'location': '$location', 'word': '$word'},
#               'count': {'$sum': 1}
#               }
#          }
#     ])
#     ans = []
#     for doc in cursor:
#         ans.append(doc)
#     return jsonify(ans)

@app.route('/api/query5', methods=['GET'])
def query5():
    """
    Query to find the top 100 words occurring on tweets involving coronavirus on a per country basis.

    :return: A json representation of array of documents(ans) which is passed an argument.
    """
    en_stops = list(set(stopwords.words('english')))
    cursor = tasks_collection.aggregate([
        {"$project": {"location": "$location", "word": {"$split": ["$tweet", " "]}}},
        {"$unwind": "$word"},
        {'$project': {'word': {'$toLower': "$word"}, 'location': 1}},
        {'$match': {
            '$and': [
                {'word': {'$ne': ""}},
                {'word': {'$nin': en_stops}}
            ]}},
        {"$group": {"_id": {"location": "$location", "word": "$word"}, "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$group": {"_id": "$_id.location", "Top_Words": {"$push": {"word": "$_id.word", "total": "$total"}}}},
        {"$project": {"location": 1, "top100Words": {"$slice": ["$Top_Words", 100]}}}
    ], allowDiskUse=True)
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


# incomplete
# db.Tweets.createIndex( { tweet: "text" } );
@app.route('/api/query6', methods=['GET'])
def query6():
    """Query to find Top 10 preventive / precautionary measures suggested by WHO worldwide /country wise

    :returns: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.find(
        {"$text": {"$search": "WHO prevent measure precaution prevention preventive measures"}},
        # {
        #     '$and': [
        #         {"$text": {"$search": "WHO"}},
        #         {"$text": {"$search": "prevent measure care precaution prevention preventive measures"}}]},
        {"tweet": 1, "_id": 0}).limit(10)
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


@app.route('/api/query7', methods=['GET'])
def query7():
    """Query to find the total no. of donations received towards COVID-19 country wise, in all the affected countries

    :returns: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.aggregate(
        [{"$match": {"$text": {"$search": "donations"}}}, {"$group": {"_id": "$location", "count": {"$sum": 1}}}])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


# incomplete
@app.route('/api/query8', methods=['GET'])
def query8():
    """Query to find the ranking of impacted countries over the last 2 month on a week basis, to see who was standing
    where at any given week

    :returns: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.aggregate([
        {"$match": {"$text": {"$search": "cases"}}},
        # {pipeline to extract the number right before cases or after or anywhere in the tweet and convert to integer
        # and store it in cases variable or any other idea},
        {"$group": {"_id": "$location", "cases": {"$max": "$cases"}}},
        {"sort": {"cases": -1}}
    ])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


app.run()
