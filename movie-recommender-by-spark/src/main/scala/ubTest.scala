import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf}

object ubTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("movie").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read
      .option("header", "true") //将第一行当作表头
      .option("inferSchema", "true") //推断数据类型
      .csv("..\\data\\ratings.csv")

    data.createOrReplaceTempView("udata")

    val userItemDf = spark.sql("select * from udata limit 100")

//    userItemDf.show()

    val userItemDf2 = spark.sql("select user_id as user_id2,movie_id as movie_id2,rating as rating2 from udata")
    val joinDf = userItemDf.join(userItemDf2, userItemDf("movie_id") === userItemDf2("movie_id2")).filter("user_id <  user_id2")
    //      .groupBy("user_id", "user_id2")

    joinDf.show()
    /*
      +------+-------+------+---------+-------+--------+-------+
      |userId|movieId|rating|timestamp|userId2|movieId2|rating2|
      +------+-------+------+---------+-------+--------+-------+
      |     1|    333|   5.0|964981179|      2|     333|    4.0|
      |     1|    527|   5.0|964984002|      3|     527|    0.5|
      |     1|   1275|   5.0|964982290|      3|    1275|    3.5|
      |     1|     47|   5.0|964983815|      4|      47|    2.0|
    */

    import spark.implicits._
    //user_id，score_id
    val userScoreSum = userItemDf.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(line => pow(line.toDouble, 2)).sum))
    val df_user_sum = userScoreSum.toDF("user_id_sum", "rating_sqrt_sum")

    df_user_sum.show()
    /*
    +-----------+------------------+
    |user_id_sum|   rating_sqrt_sum|
    +-----------+------------------+
    |        273|17.378147196982766|
    |        528|28.160255680657446|
    |        584|17.916472867168917|
    |        736|16.186414056238647|
    |        456| 52.40229002629561|
    |        312| 66.83561924602779|
    */
    //定义udf  两列进行相乘
    val product_udf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)

    val df_product = joinDf.withColumn("rating_product", product_udf(col("rating"), col("rating2")))
      .select("user_id", "user_id2", "rating_product")

    val df_sim_group = df_product.groupBy("user_id", "user_id2").agg("rating_product" -> "sum").withColumnRenamed("sum(rating_product)", "rating_sum_pro")
    /*
    +-------+--------+---------------+
    |user_id|user_id2|rating_sum_pro |
    +-------+--------+---------------+
    |    296|    296 |       354832.0|
    |    467|    467 |        77126.0|
    |    691|    691 |        32030.0|
    |    675|    675 |        18856.0|
    */
    val df_sim = df_sim_group
      .join(df_user_sum,df_sim_group("user_id")===df_user_sum("user_id_sum"))
      .drop("user_id_sum")
      .withColumnRenamed("rating_sqrt_sum","rating_sqrt_user_id_sum")
      .join(df_user_sum,df_sim_group("user_id2")===df_user_sum("user_id_sum"))
      .drop("user_id_sum")
      .withColumnRenamed("rating_sqrt_sum","rating_sqrt_user_id2_sum")


    //    df_sim.show()
    //    val df_user_sum1 = df_user_sum.withColumnRenamed("rating_sqrt_sum","rating_sqrt_sum1")

    //    val df_sim =df_sim_1.join(df_user_sum,df_product("user_id2")===df_user_sum("user_id_sum")).drop("user_id_sum")

    val sim_udf = udf((pro:Double,s1:Double,s2:Double)=>pro/(s1*s2))
    val df_res = df_sim.withColumn("sim",sim_udf(col("rating_sum_pro"),col("rating_sqrt_user_id_sum"),col("rating_sqrt_user_id2_sum"))).select("user_id","user_id2","sim")
    //    df_res.show()

    spark.close()

  }

}
