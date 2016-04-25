import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * User: aarokiasamy
  * Date: 4/20/16
  * Time: 5:53 AM
  */
object Analyze2 extends App with Utils {

  if (args.length < 3) {
    println("args[0] - The original data file (e.g. StephenData500k.txt)")
    println
    println("args[1] - Validation data file (e.g. ValidationSet.csv)")
    println
    println("args[...] - Doc Distribution file (e.g. Docs-D272236-T291120-K10-I150-1461233997655).")
    println("          The one with (\"Index\", \"Topic\", \"Probability\", \"Keywords\") as a tuple")
    sys.exit()
  } else {
    println(s"Original Data file: ${args(0)}")
    println(s"Validation Data file: ${args(1)}")
    for (ii <- 2 until args.length) println(s"Doc Distribution file: ${args(ii)}")
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

  val strlen = udf((str: String) => str.length)
  val parseCompetitorIds = udf((str: String) => Option(str).filter(_.trim.nonEmpty).map(_.split(";").map(_.toInt)))
  val parseKeyword3Joined = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.mkString(" ")) // toSet to unique the list
  val trim = udf((str: String) => str.trim)

  // read the original data files
  val originalData = dfZipWithIndex(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("delimiter", "|")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(args(0))
    .withColumnRenamed("Company Id", "CompanyId")
    .withColumnRenamed("Short Name", "Name")
    .withColumnRenamed("Competitor Ids", "CompetitorIds")
    .filter(strlen('Keywords) > 0 || strlen('desc_kw) > 0))
    .select($"*",
      parseCompetitorIds($"CompetitorIds") as 'ParsedCompetitorIds,
      parseKeyword3Joined('Keywords) as 'ParsedKeywords,
      parseKeyword3Joined('desc_kw) as 'ParsedDescKeywords)

  println
  println("Original Data")
  originalData.show(5)

  // read the validation set
  val validationSet = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(args(1))
    .select($"*", trim($"RelatedYesNo") as 'Related)

  validationSet.show(5)

  for (ii <- 2 until args.length) {
    // read the Doc Distribution file
    println("===================================")
    println(s"Doc Distribution file: ${args(ii)}")
    val docDistr = sc.textFile(args(ii)).map { line =>
      val split = line.substring(1, line.length - 1).split(",")
      (split(0).toInt, split(1).toInt, split(2).toDouble, split(3))
    }.toDF("Index", "Topic", "Probability", "Keywords")

    val dataWithTopics = originalData.join(docDistr).where($"Id" === $"Index")
      .select('CompanyId, 'Name, 'Topic, 'Probability)

    println
    println("Original Data with Topic assignments")
    dataWithTopics.show(5)

    val compare = dataWithTopics.withColumnRenamed("CompanyId", "CompanyIdLeft").withColumnRenamed("Topic", "TopicLeft")
      .join(validationSet)
      .where($"CompanyIdLeft" === $"Company1Id")
      .join(dataWithTopics.withColumnRenamed("CompanyId", "CompanyIdRight").withColumnRenamed("Topic", "TopicRight"))
      .where($"Company2Id" === $"CompanyIdRight")

    println
    compare.show

    val compareCount = compare.count()
    val truePositives  = compare.filter(($"TopicLeft" === $"TopicRight") && $"Related" === lit("Y")).count
    val trueNegatives  = compare.filter(($"TopicLeft" !== $"TopicRight") && $"Related" === lit("N")).count
    val falsePositives = compare.filter(($"TopicLeft" !== $"TopicRight") && $"Related" === lit("Y")).count
    val falseNegatives = compare.filter(($"TopicLeft" === $"TopicRight") && $"Related" === lit("N")).count
    println
    println(s"Count: $compareCount")
    println(s"True Positives: $truePositives")
    println(s"True Negatives: $trueNegatives")
    println(s"False Positives: $falsePositives")
    println(s"False Negatives: $falseNegatives")
  }
  println("===================================")
}