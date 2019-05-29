# pyspark kernel script to be run on jupyter
from pyspark.sql.types import *
from pyspark.sql.functions import *
import uuid
from pyspark.sql import Row


inputCsv = "adl://plumanalytics.azuredatalakestore.net/airdna/AirDNA-Barcelona-April-2019.csv"
outputDb = "jdbc:sqlserver://plum-analytics.database.windows.net:1433;database=analytics;user=plumadmin@plum-analytics;password=Passion-unviable-jersey-0Whim-8vetch-2wren;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
properties = {
    "user": "plumadmin@plum-analytics",
    "password": "Passion-unviable-jersey-0Whim-8vetch-2wren"
}
outputTable = "hvactable"
schema = StructType([StructField("Id", StringType(), True), StructField("ListingId", StringType(), True), StructField("Title", StringType(), True)])

# Extract
#
df = spark.read.csv(inputCsv, header=True)
dfCache = df.cache()

# Transform
#
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
dfData = dfCache.selectExpr("Listing_Title as Title") \
    .withColumn("Id",uuidUdf()) \
    .withColumn('ListingId', lit(None).cast(StringType()))
dfData = dfData.select("Id", "ListingId", "Title")
dfData.registerTempTable("airdna_a")
#  t = row["Title"].replace("'", "''") if row["Title"] is not None else ""
listingDb = spark.read.jdbc(outputDb, "dbo.Listings", properties=properties)
listingDb.registerTempTable("airdna_b")
filtered  = spark.sql("select * from airdna_a where Title not in (select Title from airdna_b)")
filtered.registerTempTable("airdna_b")

# Load
#
new = listingDb.union(filtered)
new.registerTempTable("airdna_c")

spark.table("airdna_c").write.jdbc(url=outputDb, table=outputTable, mode="append", properties=properties)