// Databricks notebook source
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().appName("Movei Analytics").getOrCreate()

// COMMAND ----------

//to read the u.data file we will create an rdd
val dataRdd = spark.sparkContext.textFile("/FileStore/tables/u.data")
//calculate the number of movies that are rated on a scale of 1 to 5
val ratingCount = dataRdd.map(line => (line.split("\t")(2), 1))
.reduceByKey((x,y) => x + y)
.sortByKey()

// COMMAND ----------

ratingCount.take(5).foreach(println)

// COMMAND ----------

//creating manual schema
val mySchema = StructType(Array(
StructField("userId", IntegerType, true),
  StructField("movieId", IntegerType, true),
  StructField("rating", IntegerType, true),
  StructField("timeStamp", LongType, true)
))

// COMMAND ----------

//creating rdd of row
val lines = spark.sparkContext.textFile("/FileStore/tables/u.data")
val rddRow = lines.map{line =>
  val p = line.split("\t")
  val userId = p(0).toInt
  val movieId = p(1).toInt
  val rating = p(2).toInt
  val timeStamp = p(3).toLong
  Row(userId, movieId, rating, timeStamp)
}

// COMMAND ----------

//creating dataframe from u.data file
val df = spark.createDataFrame(rddRow, mySchema)

// COMMAND ----------

df.show(5)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

//show all columns in dataframe
df.columns

// COMMAND ----------

//number of records in the dataframe
df.count()

// COMMAND ----------

//droping timestamp column
val dfwithoutTime = df.drop(col("timeStamp"))

// COMMAND ----------

dfwithoutTime.show(5)

// COMMAND ----------

//most popular movie id
val popMovie = dfwithoutTime.groupBy("movieId").count().orderBy(desc("count"))

// COMMAND ----------

popMovie.show(5)

// COMMAND ----------

//average rating of movie
val avgRating = dfwithoutTime.groupBy("movieId").agg(avg("rating"))

// COMMAND ----------

avgRating.show(5)

// COMMAND ----------

//joining dataframes 
val joinedDf = avgRating.join(popMovie, Seq("movieId"), "inner")

// COMMAND ----------

joinedDf.show(5)

// COMMAND ----------

//renaming a column
joinedDf.withColumnRenamed("avg(rating)", "average").show(5)

// COMMAND ----------

//filtering 
val filtered = joinedDf.filter(col("count") > 100)

// COMMAND ----------

filtered.show(5)

// COMMAND ----------

//reading the u.item file that contains the movie id to its name parameter
val itemRdd = spark.sparkContext.textFile("/FileStore/tables/u.item")
val itemLines = itemRdd.map{field =>
val q = field.split('|')
val id = q(0).toInt
val name = q(1).toString
Row(id, name)
}

// COMMAND ----------

val movieSchema = StructType(Array(
StructField("movieId", IntegerType, true),
StructField("name", StringType, true)
))

// COMMAND ----------

val movieName = spark.createDataFrame(itemLines, movieSchema)

// COMMAND ----------

movieName.show(5)

// COMMAND ----------

val combined = dfwithoutTime.join(movieName, Seq("MovieId"), "inner")

// COMMAND ----------

combined.show(5)

// COMMAND ----------

//popular movie with name
val popMovieName = combined.groupBy("name").count().orderBy(desc("count"))

// COMMAND ----------

popMovieName.show(5)

// COMMAND ----------

//most rated 
combined.groupBy("name").agg(max("rating")).show(5)
