package lda

import org.apache.spark.{SparkConf, SparkContext}

/**
  * User: aarokiasamy
  * Date: 4/20/16
  * Time: 5:53 AM
  */
object DataPrep extends App with Utils {

  if (args.length != 2) {
    println("args[0] - The original data file (e.g. StephenData500k.txt)")
    println
    println("args[1] - The output keywords file (e.g. keywords.txt)")
    println
    sys.exit()
  } else {
    println(s"Original Data file: ${args(0)}")
    run(args(0), args(1))
  }

  def run(dataFile: String, keywordsFile: String) = {

    setProperties()

    val sparkConf = new SparkConf().setAppName(s"DataPrep")
    if (System.getenv("MASTER") == null || System.getenv("MASTER").length == 0) {
      System.out.println("***** MASTER not set. Using local ******")
      sparkConf.setMaster("local[*]")
    } else {
      println("***** MASTER set to '" + System.getenv("MASTER") + "' ******")
    }
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(dataFile)
      .withColumnRenamed("Competitor Ids", "CompetitorIds")

    df.printSchema()
    df.show(5)

    val strlen = udf((str: String) => if (str == null) 0 else str.trim.length)

    val totalCount = df.count
    val competitorIdsCount = df.filter(strlen('CompetitorIds) > 0).count
    val keywordsCount = df.filter(strlen('Keywords) > 0).count
    val descKwCount = df.filter(strlen('desc_kw) > 0).count
    val someKwCount = df.filter(strlen('Keywords) > 0 || strlen('desc_kw) > 0).count
    val competitorIdsAndKeywordsCount = df.filter(strlen('CompetitorIds) > 0 && (strlen('Keywords) > 0 || strlen('desc_kw) > 0)).count

    println("Total Records: " + totalCount)
    println("Records with CompetitorIds: " + competitorIdsCount)
    println("Records with Keywords: " + keywordsCount)
    println("Records with Description Keywords: " + descKwCount)
    println("Records with Some Keywords: " + someKwCount)
    println("Records with CompetitorIds and Some Keywords: " + competitorIdsAndKeywordsCount)

    // Three options for keywords
    // 1: recurring payment solution -> recurring payment solution
    // 2. recurring payment solution -> recurring_payment_solution (each keyword is a token)
    // 3. utility billing, usage billing, online usage metering, cloud metering -> utility billing usage online metering cloud (every work gets only one representation)
    val parseKeyword1 = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";"))
    val parseKeyword2 = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9;]", "_").split(";"))
    val parseKeyword3 = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.toList) // toSet to unique the list

    val parseKeyword3Joined = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.mkString(" ")) // toSet to unique the list

    val keywords = df.filter(strlen('Keywords) > 0 || strlen('desc_kw) > 0)
      .select(parseKeyword3Joined('Keywords) as 'ParsedKeywords, parseKeyword3Joined('desc_kw) as 'ParsedDescKeywords)
      .select(concat($"ParsedKeywords", lit(" "), $"ParsedDescKeywords") as 'AllKeywords)
    keywords.repartition(1).write.text(keywordsFile)

    println(s"Count: ${keywords.count()}")
    keywords.show(5, truncate = false)

    sc.stop()
  }
}
