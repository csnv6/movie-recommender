import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, desc, expr, udf}

/**
 * @author CSN
 * @date 2023/4/3 9:57
 * @version 1.0
 */
object UserBased {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ub-cf").master("local[3]").getOrCreate()

    // 获取数据
    val data = spark.read
      .option("header", "true") //将第一行当作表头
      .option("inferSchema", "true") //推断数据类型
      .csv("hdfs://nameservice1/test/data/ratings.csv")


    // 打印数据集的类型信息
    data.printSchema()
    // 打印数据集的统计信息
    data.describe().show()


    data.createOrReplaceTempView("udata")
    val userItemDf = spark.sql("select * from udata")
    val userItemDf2 = spark.sql("select user_id as user_id2,movie_id as movie_id2,rating as rating2 from udata")
    userItemDf.show(10)


    val joinDf = userItemDf.join(userItemDf2, userItemDf("movie_id") === userItemDf2("movie_id2")).filter("user_id < user_id2")
    import breeze.numerics.{pow, sqrt}
    import spark.implicits._
    //user_id，rating
    val userSumDf = userItemDf.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      // 求每个用户的所有rating的平方和开方
      .mapValues(x => sqrt(x.toArray.map(line => pow(line.toDouble, 2)).sum))
      .toDF("user_id_sum", "rating_sqrt_sum")

    //定义udf  两列进行相乘
    val productUdf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)
    // 求同一部电影，不同的两个用户的评分积
    val productDf = joinDf.withColumn("rating_product", productUdf(col("rating"), col("rating2")))
      .select("user_id", "user_id2", "rating_product")

    // 求两个用户之间所有共同看过的电影的评分和
    val userGroupDf = productDf.groupBy("user_id", "user_id2")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)", "rating_sum_pro")

    val userRatingDf = userGroupDf
      .join(broadcast(userSumDf),userGroupDf("user_id")===userSumDf("user_id_sum"))
      .drop("user_id_sum")
      .withColumnRenamed("rating_sqrt_sum","rating_sqrt_user_id_sum")
      .join(broadcast(userSumDf),userGroupDf("user_id2")===userSumDf("user_id_sum"))
      .drop("user_id_sum")
      .withColumnRenamed("rating_sqrt_sum","rating_sqrt_user_id2_sum")

    val simUdf = udf((pro:Double,s1:Double,s2:Double)=>pro/(s1*s2))

    val simDf = userRatingDf.withColumn("sim",simUdf(col("rating_sum_pro"),col("rating_sqrt_user_id_sum"),col("rating_sqrt_user_id2_sum")))
      .select("user_id","user_id2","sim")
    simDf.show(10) // 两个用户之间的相似度


    // 获取电影数据
    val movieData = spark.read
      .option("header", "true") //将第一行当作表头
      .option("inferSchema", "true") //推断数据类型
      .csv("hdfs://nameservice1/test/data/movies.csv")
    movieData.describe().show()

    movieData.createOrReplaceTempView("movie")
    // 限制相似度大于0.6，并保留4位小数即可
    val userFilter = simDf.where("user_id = 130")
      .where("sim >= 0.6")
      .orderBy(desc("sim"))
      .limit(10)
//      .select("user_id2","sim")
      .select('user_id2,expr("round(sim,4)") as "sim")

    val a = data.join(broadcast(userFilter),userFilter("user_id2") === data("user_id"))
      .select("movie_id","rating","sim")

    a.createOrReplaceTempView("a")
    val movieTop = spark.sql(
      """
        |SELECT /*+ broadcast(b) */
        |       a.movie_id
        |       ,sum(rating*sim)/sum(sim)
        |FROM a
        |LEFT JOIN
        |(
        |	SELECT  movie_id
        |	FROM udata
        |	WHERE user_id = 130
        |)b
        |ON a.movie_id = b.movie_id
        |WHERE b.movie_id is null
        |group by a.movie_id
        |order by sum(rating*sim)/sum(sim) desc
        |limit 3
        |""".stripMargin)
    // 得出预测评分

    // 为防止预测评分出现单个相似用户评价不具备可靠性，过滤单个用户的movie
    val movieTop2 = spark.sql(
      """
        |SELECT /*+ broadcast(b) */
        |       a.movie_id
        |       ,sum(rating*sim)/sum(sim) as rating
        |FROM a
        |LEFT JOIN
        |(
        |	SELECT  movie_id
        |	FROM udata
        |	WHERE user_id = 130
        |)b
        |ON a.movie_id = b.movie_id
        |WHERE b.movie_id is null
        |group by a.movie_id
        |having count(*) > 1
        |""".stripMargin)
    val movieTopN = movieTop2.orderBy(desc("rating")).limit(10).select('movie_id,expr("round(rating,2)") as "rating")
    movieTopN.show()

    movieTopN.join(movieData,movieData("movie_id") === movieTopN("movie_id")).select("title").show(false)

  }

}
