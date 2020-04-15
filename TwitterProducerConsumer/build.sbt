name := "TwitterProjectFinal1"

version := "0.1"

scalaVersion := "2.13.1"
// https://mvnrepository.com/artifact/org.scala-sbt/sbt
libraryDependencies += "org.scala-sbt" % "sbt" % "1.3.0" % "provided"


// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml_2.12
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.12" % "1.0.6"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"


//// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
//libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30" % Test


// https://mvnrepository.com/artifact/com.twitter/hbc
libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30"

// https://mvnrepository.com/artifact/com.googlecode.json-simple/json-simple
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.2" % "provided"
//
//
//libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"
//libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
//
//
//libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2"
//
//
//libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.1"
//
//
//libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.4"
