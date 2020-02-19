
name := "sparkTwitterNew"

version := "0.1"

scalaVersion := "2.11.1"
val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6",
  "org.twitter4j" % "twitter4j-core" % "4.0.6")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
)

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-server" % "1.2.2" excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "org.apache.hbase" % "hbase-client" % "1.2.2" excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "org.apache.hbase" % "hbase-common" % "1.2.2" excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" excludeAll ExclusionRule(organization = "javax.servlet")
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + module.revision + "." + artifact.extension
}