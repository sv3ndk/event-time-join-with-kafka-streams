
lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.3",
      version      := "0.1.0-SNAPSHOT"
    )),

    name := "event-time-join-akka-streams",

    resolvers += "confluent repo" at "http://packages.confluent.io/maven/",

    libraryDependencies ++= Seq (
      "org.scalatest" %% "scalatest" % "3.0.3" % "test",
      "org.apache.kafka" % "kafka-streams" % "0.11.0.0-cp1",
      "org.apache.kafka" % "kafka-clients" % "0.11.0.0-cp1",
      "com.typesafe.play" %% "play-json" % "2.6.0"
    )

  )
