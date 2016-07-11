import blueeyes.Blueeyes._

lazy val akka_testing = project settings (commonSettings: _*) dependsOn util
lazy val bkka         = project settings (commonSettings: _*) dependsOn util
lazy val blueeyes     = project in file(".") settings (commonSettings: _*) aggregate (util, json, akka_testing, bkka, core, test)
lazy val core         = project settings (commonSettings: _*) dependsOn (util, json, bkka, akka_testing)
lazy val json         = project settings (commonSettings: _*)
lazy val test         = project settings (commonSettings: _*) dependsOn core
lazy val util         = project settings (commonSettings: _*)
