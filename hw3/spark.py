import findspark
findspark.init()
import pyspark
import sys
if len(sys.argv) != 3:
        raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")
inputUri=sys.argv[1]
outputUri=sys.argv[2]
def myMapFunc(x): # takes an input, provides an output pairing
        return (len(x), 1)
        
def myReduceFunc(v1, v2): # Merge two values with a common key - operation must be assoc. and commut.
        return v1 + v2
sc = pyspark.SparkContext()
print("Spark Context initialized.")
# textFile --> take the address of a text file, return it as an RDD (hadoop dataset) of strings
lines = sc.textFile(sys.argv[1])
print(lines.count())
# # Flatmap --> Apply a function to each element of the dataset, then flatten the result.
# words = lines.flatMap(lambda line: line.split())
wordCounts = lines.map(myMapFunc).reduceByKey(myReduceFunc)
print("Operations complete.")
wordCounts.coalesce(1, shuffle = True).saveAsTextFile(sys.argv[2])
print("Output saved as text file.")
