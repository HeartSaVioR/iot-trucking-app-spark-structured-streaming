
name := "iot-trucking-app-spark-structured-streaming"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal
resolvers ++= List("Local Maven Repository" at "file:///" + Path.userHome.absolutePath + "/.m2/repository")
resolvers ++= List("Hortonworks Releases" at "http://repo.hortonworks.com/content/repositories/releases/", "Jetty Releases" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/")
resolvers ++= List("Hortonworks Private Releases" at "http://nexus-private.hortonworks.com/nexus/content/groups/public/")

// scallop is MIT licensed
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0-SNAPSHOT" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0-SNAPSHOT" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.0-SNAPSHOT" excludeAll(
  ExclusionRule(organization = "org.spark-project.spark", name = "unused"),
  ExclusionRule(organization = "org.apache.spark", name = "spark-streaming"),
  ExclusionRule(organization = "org.apache.kafka", name = "kafka-clients"),
  ExclusionRule(organization = "org.apache.hadoop")
)
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1" excludeAll(
  ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
  )

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

target in assembly := file("build")

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}.jar"
