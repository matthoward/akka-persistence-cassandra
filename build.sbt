organization := "com.github.krasserm"

name := "akka-persistence-cassandra-12"

version := "0.4"

scalaVersion := "2.10.0"

crossScalaVersions := Seq("2.11.2", "2.11.0")

fork in Test := true

javaOptions in Test += "-Xmx2500M"

parallelExecution in Test := false

credentials += Credentials("deploymentRepo", "http://nexus.dspdev.wmg.com:8080/nexus/content/repositories/releases/", "admin", "CloudFoundry")

publishTo := Some("deploymentRepo" at "http://nexus.dspdev.wmg.com:8080/nexus/content/repositories/releases/")

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "1.0.1",
  "org.apache.commons"      % "commons-io"                        % "1.3.2",
  "com.typesafe.akka"      %% "akka-persistence-experimental"     % "2.3.6",
  "com.typesafe.akka"      %% "akka-persistence-tck-experimental" % "2.3.6"   % "test",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"   % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "1.2.0.1" % "test" exclude("com.datastax.cassandra","cassandra-driver-core") exclude("org.apache.cassandra","cassandra-all"),
  "org.apache.cassandra" % "cassandra-all"  % "1.2.13"
)


