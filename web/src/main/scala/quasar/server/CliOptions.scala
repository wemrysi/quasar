/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.server

import quasar.Predef._
import quasar.build.BuildInfo

import scopt.OptionParser

/** Command-line options supported by Quasar. */
final case class CliOptions(
  config: Option[String],
  contentLoc: Option[String],
  contentPath: Option[String],
  contentPathRelative: Boolean,
  openClient: Boolean,
  port: Option[Int])

object CliOptions {
  val default: CliOptions =
    CliOptions(None, None, None, false, false, None)

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  val parser: OptionParser[CliOptions] = new OptionParser[CliOptions]("quasar") {
    head("quasar", BuildInfo.version)

    opt[String]('c', "config") action { (x, c) =>
      c.copy(config = Some(x))
    } text("path to the config file to use")

    opt[String]('L', "content-location") action { (x, c) =>
      c.copy(contentLoc = Some(x))
    } text("location where static content is hosted")

    opt[String]('C', "content-path") action { (x, c) =>
      c.copy(contentPath = Some(x))
    } text("path where static content lives")

    opt[Unit]('r', "content-path-relative") action { (_, c) =>
      c.copy(contentPathRelative = true)
    } text("specifies that the content-path is relative to the install directory (not the current dir)")

    opt[Unit]('o', "open-client") action { (_, c) =>
      c.copy(openClient = true)
    } text("opens a browser window to the client on startup")

    opt[Int]('p', "port") action { (x, c) =>
      c.copy(port = Some(x))
    } text("the port to run Quasar on")

    help("help") text("prints this usage text")
  }
}
