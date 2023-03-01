import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf}

object ubTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("movie").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("checkpoint")

    val data = spark.read
      .option("header", "true") //将第一行当作表头
      .option("inferSchema", "true") //推断数据类型
      .csv("..\\data\\ratings.csv")

    data.createOrReplaceTempView("udata")

    val userItemDf = spark.sql("select * from udata")

//    userItemDf.show()

    val userItemDf2 = spark.sql("select user_id as user_id2,movie_id as movie_id2,rating as rating2 from udata")
    val joinDf = userItemDf.join(userItemDf2, userItemDf("movie_id") === userItemDf2("movie_id2")).filter("user_id < user_id2")
/* select a.user_id,a.movie_id,a.rating,b.user_id as user_id2,b.rating as rating2 from ratings a join ratings b where a.movie_id = b.movie_id and a.user_id <> b.user_id */
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
      // 求每个用户的所有rating的平方和开方
      .mapValues(x => sqrt(x.toArray.map(line => pow(line.toDouble, 2)).sum))
    val df_user_sum = userScoreSum.toDF("user_id_sum", "rating_sqrt_sum")
/* SELECT user_id,sqrt(sum(power(rating,2))) from ratings GROUP BY user_id */
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

    // 求同一部电影，不同的两个用户的评分积
    val df_product = joinDf.withColumn("rating_product", product_udf(col("rating"), col("rating2")))
      .select("user_id", "user_id2", "rating_product")

    // 求两个用户之间所有共同看过的电影的评分和
    val df_sim_group = df_product.groupBy("user_id", "user_id2")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)", "rating_sum_pro")
    /* select user_id,user_id2,sum(rating*rating2) from user_movie group by user_id,user_id2 */
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


    df_sim.show()
    /* select user_id,user_id2,rating_sum_pro,sum(rating_sqrt_user_id_sum),sum(rating_sqrt_user_id2_sum)
       from(
         select  a.user_id,user_id2,rating_sum_pro,rating_sqrt_sum as rating_sqrt_user_id_sum,0
         from rating_pro a join user_rating b
         where a.user_id = b.user_id
         union all
         select  a.user_id,user_id2,rating_sum_pro,0,rating_sqrt_sum as rating_sqrt_user_id2_sum
         from rating_pro a join user_rating b
         where a.user_id2 = b.user_id
       )a group by user_id,user_id2,rating_sum_pro
    */
//    +-------+--------+--------------+-----------------------+------------------------+
//    |user_id|user_id2|rating_sum_pro|rating_sqrt_user_id_sum|rating_sqrt_user_id2_sum|
//    +-------+--------+--------------+-----------------------+------------------------+
//    |      8|     148|          12.0|      25.37715508089904|       26.32964109136317|
//    |    100|     148|          53.0|      48.69804924224378|       26.32964109136317|
//    |     29|     148|          16.0|      37.77896239972718|       26.32964109136317|
//    |    126|     148|          16.0|     21.840329667841555|       26.32964109136317|
//    |     76|     148|          38.0|      36.45545226711637|       26.32964109136317|

//    val df_user_sum1 = df_user_sum.withColumnRenamed("rating_sqrt_sum","rating_sqrt_sum1")

//    val df_sim =df_sim_1.join(df_user_sum,df_product("user_id2")===df_user_sum("user_id_sum")).drop("user_id_sum")

    val sim_udf = udf((pro:Double,s1:Double,s2:Double)=>pro/(s1*s2))
    val df_res = df_sim.withColumn("sim",sim_udf(col("rating_sum_pro"),col("rating_sqrt_user_id_sum"),col("rating_sqrt_user_id2_sum"))).select("user_id","user_id2","sim")
    df_res.show() // 两个用户之间的相似度
/* select user_id,user_id2,rating_sum_pro/(rating_sqrt_user_id_sum*rating_sqrt_user_id2_sum) from user_sim group by user_id,user_id2 */

//    +-------+--------+--------------------+
//    |user_id|user_id2|                 sim|
//    +-------+--------+--------------------+
//    |     75|     148| 0.07246315490983819|
//    |      6|     148|0.011922725107014413|
//    |     16|     148| 0.09986884189147943|
//    |     86|     148| 0.10535291235038294|
//    |     95|     148|0.025481082495231264|
//    |     93|     148|0.017815093954093454|
//    |     45|     148|0.062320200807090234|

    /*
  select movie_id,rating from test.test_ratings
  where user_id in (
      select
          user_id2
      from test.test_res
      where user_id = 130
      order by sim desc
      limit 1
  ) and movie_id not in (select movie_id from test.test_ratings where user_id = 130)
  order by rating desc limit 5
    * */

    df_res.cache()
    df_res.checkpoint()

    // 这里仅演示向一位用户进行推荐

    df_res.createOrReplaceTempView("res")

    // 获取相近的10个用户
//    spark.sql(
//      """
//        | select
//        |   user_id
//        | from res
//        | where user_id = 130
//        | order by sim
//        | limit 10
//        |""".stripMargin).show()


// 获取邻近的10个用户与其不重复的电影
    val movieDf = spark.sql(
      """
        |  select movie_id,rating from udata
        |  where user_id in (
        |      select
        |          user_id2
        |      from res
        |      where user_id = 130
        |      order by sim desc
        |      limit 10
        |  ) and movie_id not in (select movie_id from udata where user_id = 130)
        |  order by rating desc limit 5
        |""".stripMargin)

    movieDf.show()

    // 用户的相似度*评分  来进行推荐10部电影
    spark.sql(
      """
        |SELECT  a.movie_id
        |       ,sum(rating*sim)
        |FROM
        |(
        |	SELECT  a.movie_id
        |	       ,rating
        |	       ,sim
        |	FROM test.test_ratings a
        |	JOIN
        |	(
        |		SELECT  user_id2
        |		       ,sim
        |		FROM test.test_res
        |		WHERE user_id = 130
        |		ORDER BY sim desc
        |		LIMIT 10
        |	) b
        |	ON a.user_id = b.user_id2
        |)a
        |LEFT JOIN
        |(
        |	SELECT  movie_id
        |	FROM test.test_ratings
        |	WHERE user_id = 130
        |)b
        |ON a.movie_id = b.movie_id
        |WHERE b.movie_id is null
        |group by a.movie_id
        |""".stripMargin)

    /*
SELECT  a.movie_id
       ,rating*sim
FROM
(
	SELECT  a.movie_id
	       ,rating
	       ,sim
	FROM test.test_ratings a
	JOIN
	(
		SELECT  user_id2
		       ,sim
		FROM test.test_res
		WHERE user_id = 130
		ORDER BY sim desc
		LIMIT 10
	) b
	ON a.user_id = b.user_id2
)a
LEFT JOIN
(
	SELECT  movie_id
	FROM test.test_ratings
	WHERE user_id = 130
)b
ON a.movie_id = b.movie_id
WHERE b.movie_id is null
*/

    spark.close()

  }

}
