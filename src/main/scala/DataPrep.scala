import org.apache.spark.{SparkContext, SparkConf}

/**
  * User: aarokiasamy
  * Date: 4/20/16
  * Time: 5:53 AM
  */
object DataPrep extends App {
  val sparkConf = new SparkConf().setAppName(s"DataPrep")
  if (System.getenv("MASTER") == null || System.getenv("MASTER").length == 0) {
    System.out.println("***** MASTER not set. Using local ******")
    sparkConf.setMaster("local[*]")
  } else {
    println("***** MASTER set to '" + System.getenv("MASTER") + "' ******")
  }
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("delimiter", "|")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("data/StephenData500k.txt")

  df.printSchema()

  val strlen = udf((str: String) => str.length)

  // Three options for keywords
  // 1: recurring payment solution -> recurring payment solution
  // 2. recurring payment solution -> recurring_payment_solution (each keyword is a token)
  // 3. utility billing, usage billing, online usage metering, cloud metering -> utility billing usage online metering cloud (every work gets only one representation)
  val parseKeyword1 = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";"))
  val parseKeyword2 = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9;]", "_").split(";"))
  val parseKeyword3 = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.toList) // toSet to unique the list

  val parseKeyword3Joined = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.mkString(" ")) // toSet to unique the list

  val keywords = df.filter(strlen('Keywords) > 0).select(parseKeyword3Joined('Keywords) as 'ParsedKeywords)
  keywords.repartition(1).write.text("data/keywords")
}
