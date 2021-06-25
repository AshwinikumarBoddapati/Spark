package SparkPack

import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._

object SparkObj{ 
	def main(args:Array[String]):Unit={ 

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._


					val url = "https://randomuser.me/api/0.8/?results=50"
					val result = scala.io.Source.fromURL(url).mkString
					val rdd1 = sc.parallelize(List(result))
					val df = spark.read.json(rdd1)
					df.printSchema()

					val explode_df = df.withColumn("results",explode(col("results")))

					val final_df = explode_df.select(
							col("nationality"),
							col("results.user.cell").alias("cell"),
							col("results.user.dob").alias("dob"),
							col("results.user.email").alias("email"),
							col("results.user.gender").alias("gender"),
							col("results.user.location.city").alias("city"),
							col("results.user.location.state").alias("state"),
							col("results.user.location.street").alias("street"),
							col("results.user.location.zip").alias("zip"),
							col("results.user.md5").alias("md5"),
							col("results.user.name.first").alias("first_name"),
							col("results.user.name.last").alias("last_name"),
							col("results.user.name.title").alias("title"),
							col("results.user.password").alias("password"),
							col("results.user.phone").alias("phone"),
							col("results.user.picture.large").alias("large_picture"),
							col("results.user.picture.medium").alias("medium_picture"),
							col("results.user.picture.thumbnail").alias("thumbnail"),
							col("results.user.registered").alias("registered"),
							col("results.user.salt").alias("salt"),
							col("results.user.sha1").alias("sha1"),
							col("results.user.sha256").alias("sha256"),
							col("results.user.username").alias("username"),
							col("seed"),
							col("version")
							)
					final_df.show(false)
					println("=========count=======")
					println(final_df.count())

	}
}