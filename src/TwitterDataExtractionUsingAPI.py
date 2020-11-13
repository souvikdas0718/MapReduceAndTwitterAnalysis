import tweepy
import json
import re
import pymongo

client = pymongo.MongoClient("mongodb+srv://root:root@datacluster.j83b5.mongodb.net/?retryWrites=true&w=majority")

apiKey = "C9Xtzla71Nufhp1ubkkkDIgFI"
apiSecretKey = "3DskwkOLGZwGataObUYeaevmNpYF8uqFqBVYbhdSFplVFJx4vz"
accessToken = "2332397412-1z56TMrEhEUMKx4lqwNgViK2gAI5F0lPLBOq7YI"
accessSecretToken = "6DkH4uRzfQ0ob2WPZ7MfIxJ8tBBAKlCwyiNJ6G4cWk0mN"
streamList = []
tweetList = []
auth = tweepy.OAuthHandler(apiKey, apiSecretKey)
auth.set_access_token(accessToken, accessSecretToken)
api = tweepy.API(auth, wait_on_rate_limit=True)
processDbData = []


def getRequiredTweetData(tweet):
    return {'created_at': str(tweet.created_at),
            'username': tweet.user.screen_name, 'location': tweet.user.location,
            'tweet': tweet.text, 'likes': tweet.favorite_count, 'followers': tweet.user.followers_count,
            'following': tweet.user.friends_count}


def checkTweets():
    keywords = ["Storm", "Winter", "Canada", "Temperature", "Flu", "Snow", "Indoor", "Safety"]
    searchTweetList = []
    for word in keywords:
        for tweet in tweepy.Cursor(api.search, lang='en', q=word).items(250):
            searchTweetList.append(
                getRequiredTweetData(tweet)
            )
    RawDb.insert_many(searchTweetList)


def checkStreamTweets():
    keywords = ["Storm", "Winter", "Canada", "Temperature", "Flu", "Snow", "Indoor", "Safety"]
    stream = tweepy.Stream(auth=api.auth, lang='en', listener=StreamListener())
    stream.filter(track=keywords, languages=['en'])
    RawDb.insert_many(tweetList)
    tweetList.clear()


class StreamListener(tweepy.StreamListener):

    def on_data(self, data):
        jsonData = json.loads(data)
        streamTweets = {'created_at': str(jsonData['created_at']),
                        'username': jsonData['user']['screen_name'],
                        'location': jsonData['user']['location'],
                        'tweet': jsonData['text'],
                        'likes': jsonData['favorite_count'],
                        'followers': jsonData['user']['followers_count'],
                        'following': jsonData['user']['friends_count']}
        tweetList.append(streamTweets)
        if len(tweetList) >= 50:
            return False


def fetchRawDataAndStoringInProcessDB():
    results = db.tweets.find({}, {'_id': 0})
    for result in results:
        cleanedData = cleanRawDBData(result)
        processDbData.append(cleanedData)
    processDb.insert_many(processDbData)


def cleanRawDBData(row):
    for column in row:
        if column == 'tweet' or (column == 'location' and row['location'] is not None):
            tweet = row[column]
            cleanTweet = "".join(
                char for char in re.sub(r'@|https?://\S+|[.]', '', tweet) if (char.isalnum() or char == ' '))
            row[column] = cleanTweet
    return row


db = client.RawDb
RawDb = db.tweets
processDb = client.ProcessDb.tweets

print("Fetching tweets")
checkTweets()
checkStreamTweets()
print("Data Stored in RawDb")
fetchRawDataAndStoringInProcessDB()
print("Cleaned Data Stored in ProcessDb")
