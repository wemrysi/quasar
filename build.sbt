import precogbuild.PlatformBuild._

def scalazVersion   = "7.2.4"
def specsVersion    = "3.8.4-quasar-20160802231651-aacb2fc4d9" // "3.8.4"
def pathyVersion    = "0.2.1"
def argonautVersion = "6.2-M3"

lazy val root = project.setup.root.noArtifacts aggregate (precog, blueeyes, yggdrasil) also (
  console in Compile <<= console in Compile in yggdrasil,
     console in Test <<= console in Test in yggdrasil
)

lazy val yggdrasil = project.setup dependsOn (blueeyes % BothScopes, precog % BothScopes) also (
  initialCommands in console in Compile := "import quasar.precog._, blueeyes._, json._",
     initialCommands in console in Test := "import quasar.precog._, blueeyes._, json._, com.precog._, bytecode._, common._, yggdrasil._"
)
lazy val blueeyes  = project.setup dependsOn (precog % BothScopes)

lazy val precog = (
  project.setup deps (

    "org.openjdk.jmh"    % "jmh-generator-annprocess" %     "1.12",
    "com.slamdata"      %% "pathy-core"               %  pathyVersion,
    "com.slamdata"      %% "pathy-argonaut"           %  pathyVersion,
    "io.argonaut"       %% "argonaut"                 % argonautVersion,
    "io.argonaut"       %% "argonaut-scalaz"          % argonautVersion,
    "com.chuusai"       %% "shapeless"                %     "2.3.1",
    "org.slf4s"         %% "slf4s-api"                %    "1.7.13",
    "org.spire-math"    %% "spire"                    %     "0.7.4",
    "org.scodec"        %% "scodec-bits"              %     "1.1.0",
    "org.scodec"        %% "scodec-scalaz"            %    "1.3.0a",
    // "io.github.jto"     %% "validation-playjson"      %      "2.0",
    // "com.typesafe.play" %% "play-json"                %     "2.5.3",
    "org.scalaz"        %% "scalaz-core"            %  scalazVersion force(),
    "org.scalaz"        %% "scalaz-effect"            %  scalazVersion,
    "org.scalaz"        %% "scalaz-concurrent"        %  scalazVersion,
    "com.slamdata"      %% "pathy-scalacheck"         %   pathyVersion   % Test,
    "org.scalacheck"    %% "scalacheck"               %     "1.12.5"     % Test force(),
    "org.scalaz.stream" %% "scalaz-stream"            %     "0.8.3a"     % Test,
    "org.specs2"        %% "specs2-scalacheck"        %   specsVersion   % Test,
    "org.specs2"        %% "specs2-core"              %   specsVersion   % Test
  )
)

lazy val benchmark = project.setup dependsOn (blueeyes % BothScopes) enablePlugins JmhPlugin also (
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
