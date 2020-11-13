import pymongo


client = pymongo.MongoClient(
    "mongodb+srv://root:root@datacluster.j83b5.mongodb.net/RawDb?retryWrites=true&w=majority")
db = client.RawDb
