import Dependencies._

name := "coordinator"

libraryDependencies ++= Seq(
  c3po,
  slf4jLog4j12,
  metricsScala,
  socrataThirdPartyUtils,
  jna,
  opencsv,
  clojure,

  TestDeps.scalaCheck % "test",
  TestDeps.scalaMock  % "test"
)

assembly/test := {}

assembly/mainClass := Some("com.socrata.datacoordinator.Launch")

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value

// not setting "publish / skip" because soql-postgres-adapter uses this package in its tests.

enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion,git.gitHeadCommit)

buildInfoPackage := "com.socrata.datacoordinator"

buildInfoOptions += BuildInfoOption.ToJson
