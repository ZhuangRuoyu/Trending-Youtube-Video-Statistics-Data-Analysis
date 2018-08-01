# Calculate the correlation of trending videos in two countries by categories
# In order to run this, we use spark-submit, below is the
# spark-submit  \
#   --master local[2] \
#   TrendingComparison.py
#   --input input-path
#   --output outputfile
#   --countryA countrycode
#   --countryB countrycode

from pyspark import SparkContext
import argparse
import re

# functions to be used

def extractVideosCountry(record):
    """
    Input: record (str) -- single row of CSV file

    Output: key-value pairs
        key: videoID,Category (str)
        value: 0/1 -- the videos in country A/B
        eg. ("LgVi6y5QIjM,Sports", 0)

    Global variables countryA and countryB used in this function.
    """
    try:
        # use regular expression to filter out the commas in values
        recordList = re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", record)
        vId = recordList[0] # video ID
        cat = recordList[5] # category name
        country = recordList[-1] # country code
        if country == countryA:
            return (vId + "," + cat, 0)
        elif country == countryB:
            return (vId + "," + cat, 1)
        else:
            return ()
    except:
        return ()

def mapCatOccurs(line):
    """
    Input: key-value pairs ("vId,cat", 0/1)
           eg. ("LgVi6y5QIjM,Sports", {0,1}), ("V1zTJIfGKaA,Sports", {0,None}), ...

    Output: key-value pairs
        key: category (str)
        value: 0 -- this video appears in country A only
               1 -- this video appears in both country A and B
    """
    vIdCat, countryList = line
    vId, cat = vIdCat.split(",")
    if countryList[1]==1:
        return (cat, 1) # the video in both countries A and B
    else:
        return (cat, 0) # the video in only country A

def countsByCountry(accumulatedPair, occursB):
    """
    Input:
        accumulatedPair: (countInA, countInB) # which is output of
                         function countsByCountry or countsCombiner,
                         or initial value (0, 0)
        occursB: 1/0 # which is output of mapCatOccurs, 0 -- appears
                 in country A only; 1 -- appears in both countries

    Output: (countInA, countInB)
    """
    countInA, countInB = accumulatedPair
    countInA += 1 # count the number of input, i.e. the occurrence in country A
    countInB += occursB # sum the values of input, i.e. the occurence in country B
    return (countInA, countInB)

def countsCombiner(accumulatedPair1, accumulatedPair2):
    """
    Input: (accumulatedPair1, accumulatedPair2) # both are outputs of
           function countsByCountry or countsCombiner or initial value
           (countInA, countInB)

    Output: (countInA, countInB)
    """
    countInA1, countInB1 = accumulatedPair1
    countInA2, countInB2 = accumulatedPair2
    return (countInA1 + countInA2, countInB1 + countInB2)

def mapOutput(line):
    """
    Map the results into the format required
    Input: key-value pairs
           (cat, {countInA, countInB})
           eg. (Sports, {163, 27})

    Output: key-value pairs
            (cat, total: countInA; percentageB in countryB)
            eg. (Sports, total: 163; 16.6% in US)
    """
    cat, counts = line
    pct = 100*counts[1]/counts[0]
    result = "total: " + str(counts[0]) + "; " + "{0:.1f}%".format(pct) + " in " + countryB
    return (cat, result)

# the main method begins here

if __name__ == "__main__":

    # input arguments needed
    sc = SparkContext(appName="Trending Correlation between Two Countries by Category")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path", default='hdfs://.../')
    parser.add_argument("--output", help="the output path", default='SparkTestOut')
    parser.add_argument("--countryA", help="the first country in the comparison", default='GB')
    parser.add_argument("--countryB", help="the second country to be compared", default='US')
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    countryA = args.countryA
    countryB = args.countryB

    # read the data and category mapping files
    dataAll = sc.textFile(input_path + "ALLvideos.csv")
    # extract the video id, country and category
    # then filter out duplicates and empty tuples
    idCat = dataAll.map(extractVideosCountry).distinct().filter(lambda x : len(x)>0)
    idCatA = idCat.filter(lambda x : x[1]==0) # filter tuples in country A
    idCatB = idCat.filter(lambda x : x[1]==1) # filter tuples in country B

    # left join, with all tuples in country A and overlapping tuples in country B
    joinedIdCat = idCatA.leftOuterJoin(idCatB)
    catOccrs = joinedIdCat.map(mapCatOccurs)
    # count the occurences by aggregateByKey
    catCorrelation = catOccrs.aggregateByKey((0,0), countsByCountry, countsCombiner).map(mapOutput)
    # format the results
    catCorrelation.saveAsTextFile(output_path)
