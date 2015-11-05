package quasar
package api.services

import Predef._

import org.http4s.{StaticFile, MediaType}
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.server.HttpService

import scalaz.concurrent.Task

object welcome {

  def resource(path: String): Task[String] = Task.delay {
    scala.io.Source.fromInputStream(getClass.getResourceAsStream(path), "UTF-8").getLines.toList.mkString("\n")
  }

  def service: HttpService = HttpService {
    case GET -> Root =>
      resource("/quasar/api/index.html").flatMap { html =>
        Ok(html
          .replaceAll("__version__", build.BuildInfo.version)
        ).withContentType(Some(`Content-Type`(MediaType.`text/html`)))
      }
    case GET -> Root / path =>
      StaticFile.fromResource("/quasar/api/" + path).fold(NotFound())(Task.now)
  }
}
