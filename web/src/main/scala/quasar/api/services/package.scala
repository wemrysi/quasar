package quasar.api

import quasar.Predef._

import argonaut._
import argonaut.Argonaut._
import org.http4s.argonaut._

import org.http4s.{EntityEncoder, Response}
import org.http4s.dsl._
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}

import quasar.fs.FileSystemError._
import quasar.fs.PathError2.{InvalidPath0, PathExists0, PathNotFound0}
import quasar.fs._
import quasar.fp._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

import pathy.Path._

package object services {
  // TODO: Polish this up
  def fileSystemErrorResponse(error: FileSystemError): Task[Response] =
    error.fold(
      pathErrorResponse,
      (logicalPlan, plannerError) => BadRequest("Planner error"),
      unknownReadHandle => BadRequest("unknow read handle"),
      unknownWriteHandle => BadRequest("unknow write handle"),
      partialWrite => BadRequest("Partial write"),
      (data, str) => BadRequest("some other thing")
    )

  def pathErrorResponse(error: PathError2): Task[Response] =
    error.fold(
      path => Conflict("path already exists"),
      // TODO: Adjust definition of `AbsPath` in order to avoid this fold...
      path => NotFound(s"${path.fold(posixCodec.printPath,posixCodec.printPath)}: doesn't exist"),
      (path, reason) => BadRequest("invalid path")
    )

//  def failureResponse(f: Json => Task[Response], message: String): Task[Response] =
//    f(Json("error" := message))

  type FSTask[A] = FileSystemErrT[Task, A]
  /** Flatten by inserting the [[quasar.fs.FileSystemError]] into the failure case of the [[scalaz.concurrent.Task]] */
  val flatten = new (FSTask ~> Task) {
    def apply[A](t: FSTask[A]): Task[A] =
      t.fold(e => Task.fail(new RuntimeException(e.shows)), Task.now).join
  }

  def formatAsHttpResponse[S[_]: Functor,A: EntityEncoder](f: S ~> Task)(data: Process[FileSystemErrT[Free[S,?], ?], A],
                                                                         contentType: `Content-Type`,
                                                                         disposition: Option[`Content-Disposition`]): Task[Response] = {
    type F[A] = Free[S,A]
    type M[A] = FileSystemErrT[F,A]
    val trans: F ~> Task = hoistFree(f)
    val trans2: M ~> FSTask  = Hoist[FileSystemErrT].hoist(trans)
    def withFormatHeaders(resp: Response) = {
      val headers = contentType :: disposition.toList
      resp.putHeaders(headers: _*)
    }
    // Check the first element of data, if it's an error return an error response, otherwise serialize any
    // other errors among the remaining data that is sent to the client.
    data.translate(trans2).unconsOption.fold(
      fileSystemErrorResponse,
      _.fold(Ok(""))({ case (first, rest) =>
        Ok(Process.emit(first) ++ rest.translate(flatten))
      }).map(withFormatHeaders)).join
  }
}
