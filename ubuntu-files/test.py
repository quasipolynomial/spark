import findspark
findspark.init("/usr/local/spark")
from pyspark import SparkContext, SparkConf, SQLContext

num = 0

def a():
    logFile = "/usr/local/spark/README.md"
    sc = SparkContext("local", "first app")
    logData = sc.textFile(logFile).cache()
    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()
    print "Lines with a: %i, lines with b: %i" % (numAs, numBs)


def b():
    sc = SparkContext("local", "count app")
    words = sc.parallelize(
        ["scala",
         "java",
         "hadoop",
         "spark",
         "akka",
         "spark vs hadoop",
         "pyspark",
         "pyspark and spark"]
    )
    counts = words.count()
    coll = words.collect()
    fore = words.foreach(f)
    words_filter = words.filter(lambda x: 'spark' in x)
    filtered = words_filter.collect()
    words_map = words.map(lambda x: (x, 1))
    mapping = words_map.collect()
    # nums = sc.parallelize([1, 2, 3, 4, 5])
    # adding = nums.reduce("add")
    x = sc.parallelize([("spark", 1), ("hadoop", 4)])
    y = sc.parallelize([("spark", 2), ("hadoop", 5)])
    joined = x.join(y)
    final = joined.collect()
    words.cache() 
    caching = words.persist().is_cached 
    words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
    data = words_new.value 
    global num
    num = sc.accumulator(10)    
    rdd = sc.parallelize([20,30,40,50]) 
    rdd.foreach(g) 
    final = num.value 

    print "Accumulated value is -> %i" % (final)
    print "Stored data -> %s" % (data) 
    elem = words_new.value[2] 
    print "Printing a particular element in RDD -> %s" % (elem)
    print "Words got chached > %s" % (caching)
    print "Join RDD -> %s" % (final)
    # print "Adding all the elements -> %i" % (adding)
    print "Key value pair -> %s" % (mapping)
    print "Fitered RDD -> %s" % (filtered)
    print "Elements in RDD -> %s" % (coll)
    print "Number of elements in RDD -> %i" % (counts)

def f(x):
    print(x)

def g(x): 
    global num
    num += x 

b()