package quasar.api.services

import org.http4s.server.{middleware, HttpService}
import org.http4s.server.middleware.{CORS, GZip}
import org.http4s.server.syntax.ServiceOps
import org.http4s.dsl._
import quasar.Predef._
import quasar.api.Server.StaticContent
import quasar.api.{Destination, HeaderParam}
import quasar.fs.{ReadFile, WriteFile, ManageFile}

import scala.collection.immutable.ListMap
import scalaz.concurrent.Task
import scalaz._, Scalaz._
import quasar.fp._

import quasar.api._

import scala.concurrent.duration._

case class RestApi(staticContent: List[StaticContent],
                   redirect: Option[String],
                   defaultPort: Int,
                   restart: Int => Task[Unit]) {

  val fileSvcs = staticContent.map { case StaticContent(l, p) => l -> staticFileService(p) }.toListMap

  def cors(svc: HttpService): HttpService = CORS(
    svc,
    middleware.CORSConfig(
      anyOrigin = true,
      allowCredentials = false,
      maxAge = 20.days.toSeconds,
      allowedMethods = Some(Set("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
      allowedHeaders = Some(Set(Destination.name.value)))) // NB: actually needed for POST only

  def AllServices[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                               W: WriteFile.Ops[S],
                                               M: ManageFile.Ops[S]): ListMap[String,HttpService] = {
    val apiServices = ListMap(
      "/data/fs"      -> data.service(f),
      "/metadata/fs"  -> metadata.service(f),
      "/server"       -> server.service(defaultPort, restart),
      "/welcome"      -> welcome.service
    ) ∘ { service =>
      cors(GZip(HeaderParam(service.orElse {
        HttpService {
          case req if req.method == OPTIONS => Ok()
        }
      })))
    }
    apiServices ++
      staticContent.map{ case StaticContent(loc, path) => loc -> staticFileService(path)}.toListMap
      ListMap("/" -> redirectService(redirect.getOrElse("/welcome")))
  }
}
