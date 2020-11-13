import pyspark


def calculateFrequencies():
    filterWordList = ["STORM", "WINTER", "CANADA", "HOT", "COLD", "FLU", "SNOW", "INDOOR", "SAFETY", "RAIN", "ICE"]
    with pyspark.SparkContext("local", "Count PySpark Frequency") as sparkCount:
        words_pool = sparkCount.textFile("CountPySparkFrequency.txt").flatMap(lambda line: line.split(" ")).map(lambda singleWord: (singleWord.upper(), 1))
        wordCount = words_pool.reduceByKey(lambda wordOne, wordTwo: wordOne + wordTwo)
        for filterWords, frequency in wordCount.filter(lambda filteredWordCount: filteredWordCount[0] in filterWordList).toLocalIterator():
            print(u"{} : {}".format(filterWords, frequency))
        wordCount.filter(lambda count: count[0] in filterWordList).saveAsTextFile("/home/souvikdas0718/mapReduceResult")


calculateFrequencies()
