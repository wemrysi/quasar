import precogbuild.PlatformBuild._

def scalazVersion     = "7.2.4"
def specsVersion      = "3.8.4-scalacheck-1.12.5"
def scalacheckVersion = "1.12.5"

lazy val root = project.setup.root.noArtifacts aggregate (precog, fallback) dependsOn (fallback) also (
    scalacOptions in console in Compile := consoleArgs,
  initialCommands in console in Compile := "import quasar.precog._, blueeyes._, json._",
       scalacOptions in console in Test := consoleArgs,
     initialCommands in console in Test := "import quasar.precog._, blueeyes._, json._, com.precog._, bytecode._, common._, ygg._"
)

lazy val fallback = project.setup dependsOn (precog % BothScopes)

lazy val precog = project.setup deps (

  "org.mapdb"      %  "mapdb"             % "3.0.1",
  "com.chuusai"    %% "shapeless"         % "2.3.1",
  "org.spire-math" %% "spire-macros"      % "0.11.0",
  "org.scalaz"     %% "scalaz-core"       % scalazVersion force(),
  "org.scalacheck" %% "scalacheck"        % scalacheckVersion      % Test force(),
  "org.specs2"     %% "specs2-scalacheck" % specsVersion           % Test,
  "org.specs2"     %% "specs2-core"       % specsVersion           % Test
)

lazy val benchmark = project.setup dependsOn (fallback % BothScopes) enablePlugins JmhPlugin also (
                fork in Test :=  true,
      sourceDirectory in Jmh <<= sourceDirectory in Test,
       classDirectory in Jmh <<= classDirectory in Test,
  dependencyClasspath in Jmh <<= dependencyClasspath in Test,
              compile in Jmh <<= (compile in Jmh) dependsOn (compile in Test),
                  run in Jmh <<= (run in Jmh) dependsOn (Keys.compile in Jmh)
)

addCommandAlias("bench", "benchmark/jmh:run -f1 -t1")
addCommandAlias("cc", "test:compile")
addCommandAlias("tt", "test")
addCommandAlias("ttq", "testQuick")
addCommandAlias("cover", "; coverage ; test ; coverageReport")
