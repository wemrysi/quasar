import blueeyes.BlueeyesBuild._

lazy val akka_testing = project.setup dependsOn util
lazy val bkka         = project.setup dependsOn util
lazy val blueeyes     = project.setup.root.noArtifacts aggregate (util, json, bkka, core, test, common)
lazy val common       = project.setup
lazy val core         = project.setup dependsOn (util, json, bkka, common.inBothScopes)
lazy val json         = project.setup
lazy val test         = project.setup dependsOn (core, common.inTestScope)
lazy val util         = project.setup
