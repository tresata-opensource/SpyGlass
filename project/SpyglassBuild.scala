import sbt._
import sbt.Keys._

object SpyglassBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      organization := "parallelai",
      name := "spyglass",
      version := "0.1-tres-SNAPSHOT",
      scalaVersion := "2.9.2",
      retrieveManaged := true,
      retrievePattern := "[artifact](-[revision])(-[classifier]).[ext]",
      libraryDependencies ++= Seq(
        "com.twitter" %% "scalding-core" % "0.8.7-tres-SNAPSHOT" % "compile",
        "org.apache.hadoop" % "hadoop-core" % "1.0.4" % "provided",
        "org.apache.hbase" % "hbase" % "0.94.6" % "provided",
        "org.slf4j" % "slf4j-log4j12" % "1.6.6" % "provided",
        "com.novocode" % "junit-interface" % "0.8" % "test",
        "org.scalatest" %% "scalatest" % "1.8" % "test"
      ),
      resolvers ++= Seq(
        "releases" at "http://server01:8080/archiva/repository/internal",
        "snapshots"  at "http://server01:8080/archiva/repository/snapshots",
        "proxy"  at "http://oss.sonatype.org/content/repositories/proxy"
      ),
      publishMavenStyle := true,
      pomIncludeRepository := { x => false },
      publishArtifact in Test := false,
      publishArtifact in (Compile, packageDoc) := false,
      publishTo <<= version { (v: String) =>
        if (v.trim.endsWith("SNAPSHOT"))
          Some("tresata-snapshots" at "http://server01:8080/archiva/repository/snapshots")
        else
          Some("tresata-releases"  at "http://server01:8080/archiva/repository/internal")
      },
      credentials += Credentials(Path.userHome / ".m2" / "credentials_internal"),
      credentials += Credentials(Path.userHome / ".m2" / "credentials_snapshots")
    )
  )

}
