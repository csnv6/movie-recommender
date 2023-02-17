import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf}

object MyItemCf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MyItemCf").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read
      .option("header", "true") //将第一行当作表头
      .option("inferSchema", "true") //推断数据类型
      .csv("..\\data\\ratings.csv")

    data.createOrReplaceTempView("udata")

    val userItemDf = spark.sql("select user_id,cast (movie_id as bigint),cast(rating as double) from udata limit 100")
    userItemDf.show()
    /*
    * +-------+--------+------+
      |user_id|movie_id|rating|
      +-------+--------+------+
      |      1|       1|   4.0|
      |      1|       3|   4.0|
      |      1|       6|   4.0|
      |      1|      47|   5.0|
      |      1|      50|   5.0|
    * */


    val userItemDf2 = spark.sql("select user_id as user_id2,cast(movie_id as bigint) as movie_id2,cast(rating as double) as rating2 from udata")
    userItemDf2.show()

    /*
    * +--------+---------+-------+
      |user_id2|movie_id2|rating2|
      +--------+---------+-------+
      |       1|        1|    4.0|
      |       1|        3|    4.0|
      |       1|        6|    4.0|
      |       1|       47|    5.0|
      |       1|       50|    5.0|
      |       1|       70|    3.0|
      */
    val joinDf = userItemDf.join(userItemDf2, userItemDf("user_id") === userItemDf2("user_id2"))
      .filter("movie_id < movie_id2")
    // item-item-score
    joinDf.show()
    /*
    +-------+--------+------+--------+---------+-------+
    |user_id|movie_id|rating|user_id2|movie_id2|rating2|
    +-------+--------+------+--------+---------+-------+
    |      1|       1|   4.0|       1|        3|    4.0|
    |      1|       3|   4.0|       1|        6|    4.0|
    |      1|       1|   4.0|       1|        6|    4.0|
    |      1|       6|   4.0|       1|       47|    5.0|
    |      1|       3|   4.0|       1|       47|    5.0|
    |      1|       1|   4.0|       1|       47|    5.0|
     */

    import spark.implicits._

    val userScoreSum = userItemDf.rdd.map(x => (x(1).toString, x(2).toString))
      .groupByKey()
      // 求平方和的开方
      .mapValues(x => sqrt(x.toArray.map(line => pow(line.toDouble, 2)).sum))

    val df_item_sum = userScoreSum.toDF("movie_id_sum", "rating_sqrt_sum")

    //定义udf  两列进行相乘
    val product_udf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)

    val df_product = joinDf.withColumn("rating_product", product_udf(col("rating"), col("rating2")))
      .select("movie_id", "movie_id2", "rating_product")

    val df_sim_group = df_product.groupBy("movie_id", "movie_id2")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)", "rating_sum_pro")

    df_sim_group.show()
    /*
    +--------+---------+--------------+
    |movie_id|movie_id2|rating_sum_pro|
    +--------+---------+--------------+
    |       1|      367|          16.0|
    |     151|      648|          15.0|
    |     553|      673|          15.0|
    |     590|     1023|          20.0|
    |     157|     1030|          15.0|
    |     733|     1042|          16.0|
    |      50|     1049|          25.0|
     */
    //
    val df_sim = df_sim_group
      .join(df_item_sum, df_sim_group("movie_id") === df_item_sum("movie_id_sum"))
      .drop("movie_id_sum")
      .withColumnRenamed("rating_sqrt_sum", "rating_sqrt_movie_id_sum")
      .join(df_item_sum, df_sim_group("movie_id2") === df_item_sum("movie_id_sum"))
      .drop("movie_id_sum")
      .withColumnRenamed("rating_sqrt_sum", "rating_sqrt_movie_id2_sum")

    val sim_udf = udf((pro: Double, s1: Double, s2: Double) => pro / (s1 * s2))
    val df_res = df_sim.withColumn("sim", sim_udf(col("rating_sum_pro"), col("rating_sqrt_movie_id_sum"), col("rating_sqrt_movie_id2_sum")))
      .select("movie_id", "movie_id2", "sim")
    df_res.show()
    
    /*
    +--------+---------+---+
    |movie_id|movie_id2|sim|
    +--------+---------+---+
    |    1127|     1224|1.0|
    |     367|     1224|1.0|
    |     296|     1224|1.0|
    |     736|     1224|1.0|
    |    1042|     1224|1.0|
    |      50|     1224|1.0|
    |    1210|     1224|1.0|
    |     110|     1224|1.0|
    |    1213|     1224|1.0|
    |    1025|     1224|1.0|
    |    1219|     1224|1.0|
    |    1073|     1224|1.0|
    |    1089|     1224|1.0|
    |     596|     1224|1.0|
    |     157|     1224|1.0|
    |       6|     1224|1.0|
    |     316|     1224|1.0|
    |     608|     1224|1.0|
    |     661|     1224|1.0|
    |    1060|     1224|1.0|
     */

  }
}
