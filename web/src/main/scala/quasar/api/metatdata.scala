package quasar
package api

import quasar.Predef._
import quasar.fs._
import quasar.fp._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl._
import org.http4s.server._
import scalaz._
import scalaz.syntax.show._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._
import scalaz.concurrent.Task
import pathy.Path._

object metadata {
  import FileSystemError._

  def service[S[_]: Functor](f: S ~> Task)(implicit M: ManageFile.Ops[S]): HttpService = {
    def dirMetadata(d: AbsDir[Sandboxed]): M.F[Task[Response]] =
      M.ls(d).bimap(
          fileSystemErrorResponse,
          nodes => Ok(Json.obj("children" := nodes.toList.sorted)))
        .merge

    def fileMetadata(f: AbsFile[Sandboxed]): M.F[Task[Response]] =
      M.fileExists(f) map (_ ? Ok(Json.obj()) | NotFound())

    HttpService {
      case GET -> AsPathyPath(path) =>
        path.fold(dirMetadata, fileMetadata).foldMap(f).join
    }
  }

  ////

  // TODO: Other FileSystemError cases
  private def fileSystemErrorResponse(error: FileSystemError): Task[Response] =
    pathError.getOption(error).cata(pathErrorResponse, BadRequest())

  private def pathErrorResponse(error: PathError2): Task[Response] =
    failureResponse(
      error.fold(κ(Conflict[Json](_)), κ(NotFound[Json](_)), κ(BadRequest[Json](_))),
      error.shows)

  private def failureResponse(f: Json => Task[Response], message: String): Task[Response] =
    f(Json("error" := message))
}
