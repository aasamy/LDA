package lda

import java.util.Properties

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * User: aarokiasamy
  * Date: 4/21/16
  * Time: 5:03 AM
  */
trait Utils {
  def median(rdd: RDD[Double]): Double = {
    val sorted = rdd.sortBy(identity).zipWithIndex().map {
      case (v, idx) => (idx, v)
    }

    val count = sorted.count()
    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.lookup(l).head + sorted.lookup(r).head) / 2
    } else sorted.lookup(count / 2).head

    median
  }

  def dfZipWithIndex( df: DataFrame,
                      offset: Int = 0,
                      colName: String = "Id",
                      inFront: Boolean = true
                    ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))
      )
    )
  }

  def setProperties() {
    val properties: Properties = new Properties()
    properties.setProperty("log4j.rootCategory", "ERROR, console")

    // Console Logger
    properties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    properties.setProperty("log4j.appender.console.Threshold", "ERROR")
    properties.setProperty("log4j.appender.console.target", "System.err")
    properties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    properties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")

    // Settings to quiet third party logs that are too verbose
    properties.setProperty("log4j.logger.org.eclipse.jetty", "WARN")
    properties.setProperty("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper", "INFO")
    properties.setProperty("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter", "INFO")

    PropertyConfigurator.configure(properties)
  }
}
