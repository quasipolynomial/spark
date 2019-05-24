import findspark
findspark.init("/usr/local/spark")

from pyspark import SparkContext
logFile = "/usr/local/spark/README.md"  
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print "Lines with a: %i, lines with b: %i" % (numAs, numBs)

from pyspark import SparkContext, SparkConf