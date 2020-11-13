import pymongo


client = pymongo.MongoClient("mongodb+srv://root:root@datacluster.j83b5.mongodb.net/?retryWrites=true&w=majority")
dataToBeCheckedList = []


def fetchMongoDBProcessData(collection):
    results = list(collection.find({}, {"_id": 0}))
    for entry in results:
        # print(entry['tweet'])
        dataToBeCheckedList.append(entry['tweet'])


def fetchMongoDBReuterData(collection):
    results = list(collection.find({}, {"_id": 0}))
    for entry in results:
        # print(entry['News'])
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


