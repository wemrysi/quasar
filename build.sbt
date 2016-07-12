import precog.PlatformBuild._

lazy val blueeyes = ProjectRef(file("blueeyes").toURI, "core")

lazy val platform = project.setup.root.noArtifacts aggregate (blueeyes, common, yggdrasil, mimir)

/** This used to be the evaluator project.
 */
lazy val mimir = project.setup.noArtifacts dependsOn yggdrasil % BothScopes

lazy val yggdrasil = project.setup dependsOn (common % BothScopes, blueeyes % BothScopes) deps (
  "org.objectweb.howl" % "howl"        % "1.0.1-1",
  "org.slamdata"       % "jdbm"        %  "3.0.0",
  "org.spire-math"     % "spire_2.9.2" %  "0.3.0"
)

lazy val common = project.setup dependsOn blueeyes deps (

  "com.google.code.findbugs"  % "jsr305"          %  "3.0.1",
  "com.google.guava"          % "guava"           % "12.0.1",
  "com.rubiconproject.oss"    % "jchronic"        %  "0.2.6",
  "org.scalacheck"           %% "scalacheck"      %  "1.10.1"  % Test,
  "org.specs2"               %% "specs2"          % "1.12.4.1" % Test,
  "ch.qos.logback"            % "logback-classic" %  "1.0.0"   % Test
)
