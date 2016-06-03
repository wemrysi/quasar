name := "blueeyes-json"

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "org.scalaz"     %% "scalaz-core"    % "7.0-SNAPSHOT" changing(),
  "joda-time"      %  "joda-time"      % "1.6.2" % "optional",
  "com.chuusai"    % "shapeless_2.9.2" % "1.2.4" % "optional"
)
