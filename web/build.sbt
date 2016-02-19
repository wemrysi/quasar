import quasar.project._

import scala.Some
import scala.collection.Seq

mainClass in Compile := Some("quasar.server.Server")

libraryDependencies ++= Dependencies.web
