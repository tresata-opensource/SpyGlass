lazy val root = (project in file(".")).settings(
  organization := "parallelai",
  name := "spyglass",
  version := "1.0.0-tres",
  scalaVersion := "2.11.8",
  retrieveManaged := true,
  retrievePattern := "[artifact](-[revision])(-[classifier]).[ext]",
  libraryDependencies ++= Seq(
    "com.twitter" %% "scalding-core" % "0.17.2" % "compile",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
    //"org.apache.hbase" % "hbase-client" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided",
    "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "provided",
    "com.novocode" % "junit-interface" % "0.11" % "test",
    "org.scalatest" %% "scalatest" % "3.0.3" % "test",
    //"org.apache.hbase" % "hbase-client" % "1.2.0" % "test" classifier "tests",
    "org.apache.hbase" % "hbase-common" % "1.2.0" % "test" classifier "tests",
    "org.apache.hbase" % "hbase-server" % "1.2.0" % "test" classifier "tests",
    "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0" % "test",
    "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0" % "test" classifier "tests",
    "org.apache.hbase" % "hbase-hadoop2-compat" % "1.2.0" % "test",
    "org.apache.hbase" % "hbase-hadoop2-compat" % "1.2.0" % "test" classifier "tests"
  ),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  publishArtifact in Test := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishTo := {
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("tresata-snapshots" at "http://server02:8080/repository/snapshots")
    else
      Some("tresata-releases"  at "http://server02:8080/repository/internal")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_internal"),
  credentials += Credentials(Path.userHome / ".m2" / "credentials_snapshots"),
  credentials += Credentials(Path.userHome / ".m2" / "credentials_proxy")
)
