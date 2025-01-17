resolvers ++= Seq(
  Resolver.url("socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.27")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json-v3" % "3.9.1",
  "com.rojoma" %% "simple-arm-v2" % "2.1.0",
  "com.thesamet.scalapb" %% "compilerplugin" % "0.9.4"
)
