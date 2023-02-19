from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys

def reorg(spark, datadir):
    # reorganize the data, output in parquet format
    person = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema","true").load(datadir + "/person*.csv*")
    interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema","true").load(datadir + "/interest*.csv*")
    knows = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema","true").load(datadir + "/knows*.csv*")

    #1. only keep the usefully information of person
    person_pass1 = person.select(col("personId"), col("locatedIn"), col("birthday")).join(knows, "personId")

    #2. create the friend dataframe, the information is just like the ori-person, and rename the friend infromation
    friend = person.select(col("personId").alias("friendId"), col("locatedIn").alias("floc"))

    #3. remove non-local friend
    person_pass2 = person_pass1.join(friend, "friendId").filter(col("locatedIn") == col("floc")).select(col("personId"), col("locatedIn"),  \
        col("birthday"), col("friendId")).dropDuplicates()

    person_pass3 = person_pass2.join(knows.select(col("personId").alias("friendId"), col("friendId").alias("f_friendId")), "friendId") \
        .filter(col("personId") == col("f_friendId"))
    
    knows_pass2 = person_pass3.select(col("personId"), col("friendId")).dropDuplicates()

    person_pass3 = person_pass3.select("personId", "locatedIn", "birthday").dropDuplicates()

    person_pass3 = person_pass3.withColumn("bday", month(col("birthday")) * 100 + dayofmonth(col("birthday"))).select("personId", "bday")
    person_pass3.show()


    # update interest
    interest_pass2 = person_pass3.join(interest, "personId")
    interest_pass3 = interest_pass2.select(col("personId"), col("interest")).dropDuplicates()
    # interest_pass3.show()

    person_pass3.write.format("parquet").mode("overwrite").option("compression", "snappy").save(datadir + "/person_parquet")
    knows_pass2.write.format("parquet").mode("overwrite").option("compression", "snappy").save(datadir + "/knows_parquet")
    interest_pass3.write.format("parquet").mode("overwrite").option("compression", "snappy").save(datadir + "/interest_parquet")

def main():
    if len(sys.argv) < 2:
        print("Usage: reorg.py [datadir]")
        sys.exit()

    datadir = sys.argv[1]

    spark = SparkSession.builder.getOrCreate()

    reorg(spark, datadir)


if __name__ == "__main__":
    main()
