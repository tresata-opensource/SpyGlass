import sbt._
import sbt.Keys._

object SpyglassBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file(".")
  ).settings(
    Seq(
      organization := "parallelai",
      name := "spyglass",
      version := "0.7.0-tres-SNAPSHOT",
      scalaVersion := "2.11.7",
      retrieveManaged := true,
      retrievePattern := "[artifact](-[revision])(-[classifier]).[ext]",
      libraryDependencies ++= Seq(
        "com.twitter" %% "scalding-core" % "0.16.0-RC3" % "compile",
        "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
        //"org.apache.hbase" % "hbase-client" % "0.98.6-hadoop2" % "provided",
        "org.apache.hbase" % "hbase-common" % "0.98.6-hadoop2" % "provided",
        "org.apache.hbase" % "hbase-server" % "0.98.6-hadoop2" % "provided",
        "org.slf4j" % "slf4j-log4j12" % "1.6.6" % "provided",
        "com.novocode" % "junit-interface" % "0.8" % "test",
        "org.scalatest" %% "scalatest" % "2.2.6" % "test",
        //"org.apache.hbase" % "hbase-client" % "0.98.6-hadoop2" % "test" classifier "tests",
        "org.apache.hbase" % "hbase-common" % "0.98.6-hadoop2" % "test" classifier "tests",
        "org.apache.hbase" % "hbase-server" % "0.98.6-hadoop2" % "test" classifier "tests",
        "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.6-hadoop2" % "test",
        "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.6-hadoop2" % "test" classifier "tests",
        "org.apache.hbase" % "hbase-hadoop2-compat" % "0.98.6-hadoop2" % "test",
        "org.apache.hbase" % "hbase-hadoop2-compat" % "0.98.6-hadoop2" % "test" classifier "tests"
      ),
      publishMavenStyle := true,
      pomIncludeRepository := { x => false },
      publishArtifact in Test := false,
      publishArtifact in (Compile, packageDoc) := false,
      publishTo <<= version { (v: String) =>
        if (v.trim.endsWith("SNAPSHOT"))
          Some("tresata-snapshots" at "http://server02:8080/repository/snapshots")
        else
          Some("tresata-releases"  at "http://server02:8080/repository/internal")
      },
      credentials += Credentials(Path.userHome / ".m2" / "credentials_internal"),
      credentials += Credentials(Path.userHome / ".m2" / "credentials_snapshots"),
      credentials += Credentials(Path.userHome / ".m2" / "credentials_proxy")
    )
  )
}
