import precog.PlatformBuild._

def scalazVersion = "7.2.4"
def specsVersion  = "3.7"

lazy val root = project.setup.root.noArtifacts aggregate (mimir, blueeyes, yggdrasil) dependsOn (mimir, yggdrasil, blueeyes) also (
  initialCommands in console := "import blueeyes._, json._"
)

/** mimir used to be the evaluator project.
 */
lazy val mimir     = project.setup.noArtifacts dependsOn yggdrasil % BothScopes
lazy val yggdrasil = project.setup dependsOn blueeyes % BothScopes

lazy val blueeyes = (
  project.setup deps (
    "org.joda"           % "joda-convert"      %    "1.8.1",
    "joda-time"          % "joda-time"         %    "2.9.4",
    "com.chuusai"       %% "shapeless"         %    "2.3.1",
    "org.slf4s"         %% "slf4s-api"         %   "1.7.13",
    "org.spire-math"    %% "spire"             %    "0.7.4",
    "org.scodec"        %% "scodec-bits"       %    "1.1.0",
    "org.scodec"        %% "scodec-scalaz"     %    "1.3.0a",
    "org.scalaz"        %% "scalaz-effect"     % scalazVersion,
    "org.scalaz"        %% "scalaz-concurrent" % scalazVersion,
    "org.scalacheck"    %% "scalacheck"        %    "1.12.5"    % Test,
    "org.scalaz.stream" %% "scalaz-stream"     %    "0.8.3a"    % Test,
    "org.specs2"        %% "specs2-scalacheck" %  specsVersion  % Test,
    "org.specs2"        %% "specs2-core"       %  specsVersion  % Test
  )
)

lazy val all = project.setup.root.noArtifacts aggregate (blueeyes, yggdrasil, mimir) dependsOn (mimir % BothScopes)
