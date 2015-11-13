package quasar.api.services

import quasar.Predef._

import argonaut._, Argonaut._
import org.http4s._, argonaut._, dsl._
import org.http4s.server.HttpService

import scalaz._, concurrent.Task
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

import pathy.Path._

import quasar.api.AsPath
import quasar.fs._

object metadata {

  def service[S[_]: Functor](f: S ~> Task)(implicit Q: QueryFile.Ops[S]): HttpService = {
    def dirMetadata(d: AbsDir[Sandboxed]): Q.F[Task[Response]] =
      Q.ls(d).bimap(
          fileSystemErrorResponse,
          nodes => Ok(Json.obj("children" := nodes.toList.sorted)))
        .merge

    def fileMetadata(f: AbsFile[Sandboxed]): Q.F[Task[Response]] =
      Q.fileExists(f) map (_ ? Ok(Json.obj()) | NotFound(Json("error" := s"File not found: ${posixCodec.printPath(f)}")))

    HttpService {
      case GET -> AsPath(path) =>
        path.fold(dirMetadata, fileMetadata).foldMap(f).join
    }
  }
}
