import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * User: aarokiasamy
  * Date: 4/20/16
  * Time: 5:53 AM
  *
  * args[0] - Doc Distribution file (e.g. Docs-D272236-T291120-K10-I150-1461233997655).
  *           The one with ("Index", "Topic", "Probability", "Keywords") as a tuple
  *
  * args[1] - The keywords file (e.g. keywords272k.txt), the one with keywords for a document on a line.
  *           The one with "billing subscription recurring management" per line
  *
  * args[2] - The original data file (e.g. StephenData500k.txt)
  *
  */
object Analyze extends App with Utils {

  if (args.length != 3) {
    println("args[0] - Doc Distribution file (e.g. Docs-D272236-T291120-K10-I150-1461233997655).")
    println("          The one with (\"Index\", \"Topic\", \"Probability\", \"Keywords\") as a tuple")
    println
    println("args[1] - The keywords file (e.g. keywords272k.txt), the one with keywords for a document on a line.")
    println("          The one with \"billing subscription recurring management\" per line")
    println
    println("args[2] - The original data file (e.g. StephenData500k.txt)")
    sys.exit()
  } else {
    println(s"Doc Distribution file: ${args(0)}" )
    println(s"Keywords file: ${args(1)}" )
    println(s"Original Data file: ${args(2)}" )
  }

  setProperties()

  val sparkConf = new SparkConf().setAppName(s"Analyze")
  if (System.getenv("MASTER") == null || System.getenv("MASTER").length == 0) {
    System.out.println("***** MASTER not set. Using local ******")
    sparkConf.setMaster("local[*]")
  } else {
    println("***** MASTER set to '" + System.getenv("MASTER") + "' ******")
  }
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  // read the Doc Distribution file
  val docDistr = sc.textFile(args(0)).map { line =>
    val split = line.substring(1, line.length - 1).split(",")
    (split(0).toInt, split(1).toInt, split(2).toDouble, split(3))
  }.toDF("Index", "Topic", "Probability", "Keywords")

  println
  println("Doc Distribution file schema")
  docDistr.printSchema()

//  +-----+-----+
//  |topic|count|
//  +-----+-----+
//  |    0|29961|
//  |    1|31403|
//  |    2|24883|
//  |    3|26720|
//  |    4|23332|
//  |    5|25748|
//  |    6|26716|
//  |    7|26053|
//  |    8|27956|
//  |    9|26904|
//  +-----+-----+
//  docDistr.select('Topic).groupBy('Topic).count().show()

//  0 : 0.12156214047672906 : 2554
//  1 : 0.20939438661575746 : 18944
//  2 : 0.2972266327547859 : 42042
//  3 : 0.3850588788938143 : 48808
//  4 : 0.4728911250328427 : 48110
//  5 : 0.5607233711718711 : 38159
//  6 : 0.6485556173108995 : 29255
//  7 : 0.7363878634499279 : 19356
//  8 : 0.8242201095889563 : 10185
//  9 : 0.9120523557279847 : 12263
  private val rdd = docDistr.select('Probability).rdd.map(_.getDouble(0))
//  val (buckets, counts) = rdd.histogram(10)
//  for (ii <- counts.indices) {
//    println($"$ii : ${buckets(ii)} : ${counts(ii)}")
//  }
//  println($"${buckets.length-1} : ${buckets.last}")

  //  min: 0.12156214047672906, max: 0.9998846018670131, count: 269676
  println(s"median: ${median(rdd)} ${rdd.stats()}")

  // read the original docs/keywords file
  val keywords = sc.textFile(args(1)).zipWithIndex().map(x => (x._2, x._1))
  val doc = docDistr.map(r => (r.getInt(0).toLong, (r.getInt(1), r.getDouble(2), r.getString(3))))
//  doc.join(keywords).take(10).foreach(println)

  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("delimiter", "|")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(args(2))
    .withColumnRenamed("Company Id", "CompanyId")
    .withColumnRenamed("Competitor Ids", "CompetitorIds")

  val strlen = udf((str: String) => str.length)
  val parseCompetitorIds = udf((str: String) => Option(str).filter(_.trim.nonEmpty).map(_.split(";").map(_.toInt)))
  val parseKeyword3Joined = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.mkString(" ")) // toSet to unique the list

  //  val totalCount = df.count
  //  val competitorIdsCount = df.filter(strlen('CompetitorIds) > 0).count
  //  val keywordsCount = df.filter(strlen('Keywords) > 0).count
  //  val competitorIdsAndKeywordsCount = df.filter(strlen('CompetitorIds) > 0 && strlen('Keywords) > 0).count
  //
  //  println("Total Records: " + totalCount)
  //  println("Records with CompetitorIds: " + competitorIdsCount)
  //  println("Records with Keywords: " + keywordsCount)
  //  println("Records with CompetitorIds and Keywords: " + competitorIdsAndKeywordsCount)

  val ds = dfZipWithIndex(df.filter(strlen('Keywords) > 0))
    .select($"*",
      parseCompetitorIds($"CompetitorIds") as 'ParsedCompetitorIds,
      parseKeyword3Joined('Keywords) as 'ParsedKeywords)
    .join(docDistr).where($"Id" === $"Index")

  println
  println("Original Data with Topic assignments")
  ds.printSchema()
  ds.show(5)

  val rds = ds.select('CompanyId, 'Topic, 'Probability, 'ParsedCompetitorIds).filter("ParsedCompetitorIds is not null")
  //  rds.write.parquet("data/CompanyIdCompetitorIds")
  //  val rds = sqlContext.read.parquet("data/CompanyIdCompetitorIds")
  val rdsFlattened = sqlContext.createDataFrame {
    rds.flatMap {
      r =>
        r.getAs[Seq[Int]]("ParsedCompetitorIds").map {
          competitorId =>
            (r.getAs[Int]("CompanyId"), r.getAs[Int]("Topic"), r.getAs[Double]("Probability"), competitorId)
        }
    }
  }.withColumnRenamed("_1", "LeftCompanyId")
    .withColumnRenamed("_2", "LeftTopic")
    .withColumnRenamed("_3", "LeftProbability")
    .withColumnRenamed("_4", "LeftCompetitorId")

  val rdsJoined = rdsFlattened.join(rds).where($"LeftCompetitorId" === $"CompanyId")
  println
  println("Flattened and Self Joined")
  rdsJoined.show(30)

  private val count = rdsJoined.count()
  private val matchCount = rdsJoined.filter($"LeftTopic" === $"Topic").count()
  private val noMatchCount = rdsJoined.filter($"LeftTopic" !== $"Topic").count()

  println
  println("Total Records with CompetitorId: " + count)
  println("Match: " + matchCount)
  println("NO Match: " + noMatchCount)
}