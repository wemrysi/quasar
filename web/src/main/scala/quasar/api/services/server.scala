package quasar
package api.services

import Predef._

import argonaut._, Argonaut._
import org.http4s.dsl._
import org.http4s.argonaut._
import org.http4s.server.HttpService

import scalaz.concurrent.Task
import scalaz._, Scalaz._

object server {

  val nameAndVersionInfo = Json("name" := "Quasar", "version" := build.BuildInfo.version)

  /** A service that provides basic information about the webserver and exposes the ability to change on which
    * port the server is listening on.
    * @param restart A function that will restart the server on the specified port or on the default port if given None
    */
  def service(defaultPort: Int, restart: Int => Task[Unit]): HttpService = HttpService {
    case GET -> Root / "info" =>
      Ok(nameAndVersionInfo)
    case req @ PUT -> Root / "port" =>
      req.as[String].flatMap(body =>
        body.parseInt.fold(
          e => BadRequest(e.getMessage),
          // TODO: If the requested port is unavailable the server will restart
          // on a random port, this this response text may not be accurate.
          portNum => restart(portNum) *> Ok("Changed port to " + portNum)
        )
      )
    case DELETE -> Root / "port" =>
      restart(defaultPort) *> Ok("Reverted to default port " + defaultPort)
  }
}
