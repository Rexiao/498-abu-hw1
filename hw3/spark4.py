import findspark
findspark.init()
import enum
import pyspark
import sys
import functools
print(sys.argv)

if len(sys.argv) != 3:
    raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")
words = sys.argv[1].split(',')
wordSet = set(map(lambda x: x.lower(), words))
weights = sys.argv[2].split(',')
weight_dict = {}
inputUri = 'War_and_Peace.txt'
outputUri = 'output4'
for i, w in enumerate(weights):
    weight_dict[chr(ord('a') + i)] = int(w)

print(wordSet)
print(weight_dict)

def myMapFunc(x):  # takes an input, provides an output pairing
    res = []
    lower_x = x.lower()
    total_weight = None
    for word in wordSet:
        # print(word)
        # print(x)
        if word in lower_x:
        #     print('hit')
            if total_weight is None:
                total_weight = functools.reduce(
                    lambda a, b: a+weight_dict.get(b, 0), lower_x, 0)
            res.append((word, x, total_weight))
#     print(res)
    return res


# Merge two values with a common key - operation must be assoc. and commut.
def myReduceFunc(v1, v2):
#     print(v1, v2)
#     print("v1")
#     print(v1)
#     print('v2')
#     print(v2)
    _,_, w1 = v1
    _,_, w2 = v2
    if w1 >= w2:
        return v1
    return v2


sc = pyspark.SparkContext()
print("Spark Context initialized.")
# textFile --> take the address of a text file, return it as an RDD (hadoop dataset) of strings
lines = sc.textFile(inputUri)
print(lines.count())
# # Flatmap --> Apply a function to each element of the dataset, then flatten the result.
# words = lines.flatMap(lambda line: line.split())
wordCounts = lines.flatMap(myMapFunc)
wordCounts = wordCounts.keyBy(lambda x: x[0])
print(wordCounts.count())
wordCounts = wordCounts.reduceByKey(
    myReduceFunc).map(lambda x: (x[0], x[1][1]))
# wordCounts.coalesce(1, shuffle=True).saveAsTextFile('temp')
print("Operations complete.")
wordCounts.coalesce(1, shuffle=True).saveAsTextFile(outputUri)
print("Output saved as text file.")
