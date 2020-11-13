import pymongo
import re

client = pymongo.MongoClient("mongodb+srv://root:root@datacluster.j83b5.mongodb.net/?retryWrites=true&w=majority")


def fetchFromFileAndAppendToList(fileName):
    newList = []
    file = open(fileName, 'r')
    file_str = file.read()

    badCharacterPattern = re.compile(r"&#\d*;")
    file_str = badCharacterPattern.sub('', file_str)

    regex_reuter = "<REUTERS.*>((.|\n)*?)<\/REUTERS>"
    regex_text = "<TEXT.*>((.|\n)*?)<\/TEXT>"
    regex_body = "<BODY.*>((.|\n)*?)<\/BODY>"
    regex_title = "<TITLE.*>((.|\n)*?)<\/TITLE>"
    regex_date = "<DATE.*>((.|\n)*?)<\/DATE>"

    all_reuter_tags = re.findall(regex_reuter, file_str)
    for tag, _ in all_reuter_tags:

        date_tag = re.findall(regex_date, tag)
        if len(date_tag) > 0:
            date_tag = date_tag[0][0]
        else:
            date_tag = None

        text_tag = re.findall(regex_text, tag)
        body_tag = re.findall(regex_body, tag)
        if len(body_tag) > 0:
            body_tag = body_tag[0][0]
        else:
            removeTags = re.compile('<.*?>')
            cleanText = re.sub(removeTags, '', text_tag[0][0])
            body_tag = cleanText

        title_tag = re.findall(regex_title, tag)
        if len(title_tag) > 0:
            title_tag = title_tag[0][0]
        else:
            title_tag = None

        if title_tag is not None or body_tag is not None:
            newsFeed = {'Date': date_tag, 'News_Title': title_tag, 'News': body_tag}
            newList.append(newsFeed)

    return newList


newsOne = client.ReuterDb.newsOne
newsOne.insert_many(fetchFromFileAndAppendToList("reut2-009.sgm"))
print("Data inserted in newOne Collection of ReuterDb")

newsTwo = client.ReuterDb.newsTwo
newsTwo.insert_many(fetchFromFileAndAppendToList("reut2-014.sgm"))
print("Data inserted in newTwo Collection of ReuterDb")
