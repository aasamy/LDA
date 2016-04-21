lazy val root = (project in file(".")).
  settings(
    name := "Owler",
    version := "1.0",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("org.apache.spark.examples.mllib.LDAExample")
  )

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt"       % "3.4.0",
  "org.apache.spark" %% "spark-core"  % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided",
  "com.databricks"   %% "spark-csv"   % "1.4.0"
)

resolvers += Resolver.sonatypeRepo("public")
