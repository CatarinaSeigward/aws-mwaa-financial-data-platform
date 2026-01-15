name := "financial-etl"

version := "1.0.0"

scalaVersion := "2.12.18"

// Spark version compatible with AWS Glue 4.0 and local development
val sparkVersion = "3.4.1"

// Dependencies
libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // Hadoop AWS (for S3 access)
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % "provided",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.367" % "provided",

  // Configuration
  "com.typesafe" % "config" % "1.4.2",

  // Logging
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.slf4j" % "slf4j-log4j12" % "1.7.36",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test
)

// Assembly settings for creating fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".html") => MergeStrategy.first
  case x if x.endsWith(".txt") => MergeStrategy.first
  case x if x.contains("module-info") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

// Exclude provided dependencies from assembly
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { jar =>
    jar.data.getName.startsWith("spark-") ||
    jar.data.getName.startsWith("hadoop-") ||
    jar.data.getName.startsWith("scala-")
  }
}

// JAR naming
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

// Test options
Test / parallelExecution := false
Test / fork := true
Test / javaOptions ++= Seq(
  "-Xmx2G",
  "-XX:+UseG1GC"
)

// Source directories
Compile / scalaSource := baseDirectory.value / "src" / "transformation" / "scala"
Test / scalaSource := baseDirectory.value / "src" / "test" / "scala"
