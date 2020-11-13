import pymongo

client = pymongo.MongoClient("mongodb+srv://root:root@datacluster.j83b5.mongodb.net/?retryWrites=true&w=majority")
dataToBeCheckedList = []


def fetchMongoDBProcessData(collection):
    results = list(collection.find({}, {"_id": 0, "created_at": 1, "username": 1, "location": 1, "tweet": 1, "likes": 1,
                                        "followers": 1, "following": 1}))
    for entry in results:
        dataToBeCheckedList.append(entry['tweet'])


def fetchMongoDBReuterData(collection):
    results = list(collection.find({}, {"_id": 0, "Date": 1, "News_Title": 1, "News": 1}))
    for entry in results:
        dataToBeCheckedList.append(entry['News'])


def storeTheWordsInLocalFile():
    localFile = open("CountPySparkFrequency.txt", "a+")
    localFile.write(' '.join(map(str, dataToBeCheckedList)))


processDb = client.ProcessDb.tweets
newsOne = client.ReuterDb.newsOne
newsTwo = client.ReuterDb.newsTwo
fetchMongoDBProcessData(processDb)
fetchMongoDBReuterData(newsOne)
fetchMongoDBReuterData(newsTwo)
storeTheWordsInLocalFile()
