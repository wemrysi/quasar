package quasar
package api
package services

import Predef._

import argonaut._, Argonaut._
import org.http4s.dsl._
import org.http4s.argonaut._
import org.http4s.server.HttpService

import scalaz.concurrent.Task
import scalaz._, Scalaz._

object server {

  val nameAndVersionInfo = Json("name" := "Quasar", "version" := build.BuildInfo.version)

  /** A service that provides basic information about the web server and exposes the ability to change on which
    * port the server is listening on.
    * @param defaultPort The default port for this server. Will restart the server on this port
    *                    if the `DELETE` http method is used on the `port` resource.
    * @param restart A function that will restart the server on the specified port
    */
  def service(defaultPort: Int, restart: Int => Task[Unit]): HttpService = HttpService {
    case GET -> Root / "info" =>
      Ok(nameAndVersionInfo)
    case req @ PUT -> Root / "port" =>
      req.as[String].flatMap(body =>
        body.parseInt.fold(
          e => BadRequest(e.getMessage),
          portNum => Server.unavailableReason(portNum).run.flatMap { possibleReason =>
            possibleReason.map{ reason =>
              PreconditionFailed(s"Could not restart server on new port because $reason")
            }.getOrElse {
              restart(portNum).flatMap(_ => Ok("Attempted to change port to " + portNum)).handleWith {
                case e => InternalServerError("Failed to restart server on port " + portNum)
              }
            }
          }
        )
      )
    case DELETE -> Root / "port" =>
      restart(defaultPort) *> Ok("Reverted to default port " + defaultPort)
  }
}
