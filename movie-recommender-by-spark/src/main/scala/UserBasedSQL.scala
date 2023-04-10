import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserBasedSQL {

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
    userItemDf.show(10)

    val joinDf = spark.sql(
      """
        |select
        | a.user_id,
        | a.movie_id,
        | a.rating,
        | b.user_id as user_id2,
        | b.movie_id as movie_id2,
        | b.rating as rating2
        |from udata a
        |join udata b
        |on a.movie_id = b.movie_id and a.user_id < b.user_id
        |""".stripMargin)

    joinDf.show(10)
    joinDf.createOrReplaceTempView("join_df")

    // 求每个用户的所有rating的平方和开方
    val userSumDf = spark.sql(
      """
        |select
        | user_id,
        | sqrt(sum(power(rating,2))) as rating_sqrt_sum
        |from udata
        |group by user_id
        |""".stripMargin)

    userSumDf.show(10)
    userSumDf.createOrReplaceTempView("user_sum_df")

    // 求同一部电影，不同的两个用户的评分积
    val productDf = spark.sql(
      """
        |select
        | user_id,
        | user_id2,
        | rating*rating2 as rating_product
        |from join_df
        |""".stripMargin)

    productDf.show(10)
    productDf.createOrReplaceTempView("product_df")

    // 求两个用户之间所有共同看过的电影的评分和
    val userGroupDf = spark.sql(
      """
        |select
        | user_id,
        | user_id2,
        | sum(rating_product) as rating_sum_pro
        |from product_df
        |group by user_id,user_id2
        |""".stripMargin)

    userGroupDf.show(10)
    userGroupDf.createOrReplaceTempView("user_group_df")

    val userRatingDf = spark.sql(
      """
        |select
        | user_id,
        | user_id2,
        | rating_sum_pro,
        | sum(rating_sqrt_user_id_sum) as rating_sqrt_user_id_sum,
        | sum(rating_sqrt_user_id2_sum) as rating_sqrt_user_id2_sum
        |from(
        |         select  a.user_id,user_id2,rating_sum_pro,rating_sqrt_sum as rating_sqrt_user_id_sum,0
        |         from user_group_df a join user_sum_df b
        |         where a.user_id = b.user_id
        |         union all
        |         select  a.user_id,user_id2,rating_sum_pro,0,rating_sqrt_sum as rating_sqrt_user_id2_sum
        |         from user_group_df a join user_sum_df b
        |         where a.user_id2 = b.user_id
        | )a
        | group by user_id,user_id2,rating_sum_pro
        |""".stripMargin)

    userRatingDf.show(10)
    userRatingDf.createOrReplaceTempView("user_rating_df")

    // 求得两个用户之间的相似度
    val simDf = spark.sql(
      """
        |select
        | user_id,
        | user_id2,
        | rating_sum_pro/(rating_sqrt_user_id_sum*rating_sqrt_user_id2_sum) as sim
        |from user_rating_df
        |""".stripMargin)

    simDf.show(10)
    simDf.createOrReplaceTempView("sim_df")

    // 获取电影数据
    val movieData = spark.read
      .option("header", "true") //将第一行当作表头
      .option("inferSchema", "true") //推断数据类型
      .csv("hdfs://nameservice1/test/data/movies.csv")

    movieData.describe().show()
    movieData.createOrReplaceTempView("movie")

    // 限制相似度大于0.6，并保留4位小数即可
    val userFilter = spark.sql(
      """
        |select
        | user_id2,
        | round(sim,2) as sim
        |from sim_df
        |where user_id = 130 and sim >= 0.6
        |order by sim desc
        |limit 10
        |""".stripMargin)

    userFilter.show()
    userFilter.createOrReplaceTempView("user_filter")

    val filter = spark.sql(
      """
        |select
        | /*+ broadcast(b) */
        | movie_id,
        | rating,
        | sim
        |from udata a
        |join user_filter b
        |on a.user_id = b.user_id2
        |""".stripMargin)

    filter.show(10)
    filter.createOrReplaceTempView("filter")

    // 为防止预测评分出现单个相似用户评价不具备可靠性，过滤单个用户的movie
    val movieTopN = spark.sql(
      """
        |select
        | movie_id,
        | round(rating,2) as rating
        |from
        |(SELECT /*+ broadcast(b) */
        |       a.movie_id
        |       ,sum(rating*sim)/sum(sim) as rating
        | FROM filter
        | LEFT JOIN
        | (
        |	  SELECT  movie_id
        | 	FROM udata
        | 	WHERE user_id = 130
        | )b
        | ON a.movie_id = b.movie_id
        | WHERE b.movie_id is null
        | group by a.movie_id
        | having count(*) > 1
        |)a
        |order by rating desc
        |limit 10
        |""".stripMargin)

    movieTopN.show()
    movieTopN.createOrReplaceTempView("movie_topn")
    movieTopN.cache()

    spark.sql(
      """
        |select
        | title
        |from movie_topn a
        |join movie b
        |on a.movie_id = b.movie_id
        |""".stripMargin).show(false)

  }

}
