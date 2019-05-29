from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
## create a SparkSession
spark = SparkSession \
    .builder \
    .appName("join_file_with_mysql") \
    .config("spark.shuffle.service.enabled","true") \
    .config("spark.dynamicAllocation.enabled","true") \
    .config("spark.executor.cores","5") \
    .getOrCreate()
 
## create a schema for the orders file
customSchema = StructType([ \
    StructField("order_number", StringType(), True), \
    StructField("customer_number", StringType(), True), \
    StructField("order_quantity", StringType(), True), \
    StructField("unit_price", StringType(), True), \
    StructField("total", StringType(), True)])
 
## create a DataFrame for the orders files stored in HDFS
df_orders = spark.read \
    .format('com.databricks.spark.csv') \
    .options(header='true', delimiter='|') \
    .load('hdfs:///orders_file/', schema = customSchema)
 
## register the orders data as a temporary table
df_orders.registerTempTable("orders")
 
## create a DataFrame for the MySQL customer table
df_customers = spark.read.format("jdbc").options(
    url ="jdbc:mysql://bmathew-test.czrx1al336uo.ap-northeast-1.rds.amazonaws.com:3306/test",
    driver="com.mysql.jdbc.Driver",
    dbtable="customer",
    user="root",
    password="password"
).load()
 
## register the customers data as a temporary table
df_customers.registerTempTable("customers")
 
## join the DataSets
sql='''
SELECT a.first_name, a.last_name, b.order_number, b.total
FROM customers a, orders b
WHERE a.customer_number = b.customer_number
'''
 
output = spark.sql(sql)
 
## save the data into an ORC file
output.write.format("orc").save("/tmp/customer_orders")
 