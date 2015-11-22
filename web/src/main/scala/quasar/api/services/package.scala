package quasar.api

import quasar.Data
import quasar.Predef._

import org.http4s.{Charset, EntityEncoder, Response}
import org.http4s.dsl._
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}

import quasar.fs.FileSystemError._
import quasar.fs._
import quasar.fp._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

import pathy.Path._

package object services {
  // TODO: Polish this up
  def fileSystemErrorResponse(error: FileSystemError): Task[Response] =
    error match {
      case Case.PathError(e) => pathErrorResponse(e)
      case Case.PlannerError(_, _) => BadRequest("Planner error")
      case Case.UnknownReadHandle(handle) => InternalServerError(s"Unknown read handle: $handle")
      case Case.UnknownWriteHandle(handle) => InternalServerError(s"Unknown write handle: $handle")
      case Case.PartialWrite(numFailed) => InternalServerError(s"Failed to write $numFailed records")
      case Case.WriteFailed(data, reason) => InternalServerError(s"Failed to write ${data.shows} because of $reason")
    }


  def pathErrorResponse(error: PathError2): Task[Response] =
    error match {
      case PathError2.Case.PathExists(path) => Conflict(s"$path already exists")
      // TODO: Adjust definition of `AbsPath` in order to avoid this fold...
      case PathError2.Case.PathNotFound(path) => NotFound(s"${posixCodec.printPath(path)}: doesn't exist")
      case PathError2.Case.InvalidPath(path, reason) => BadRequest(s"$path is an invalid path because $reason")
    }

  type FilesystemTask[A] = FileSystemErrT[Task, A]
  /** Flatten by inserting the [[quasar.fs.FileSystemError]] into the failure case of the [[scalaz.concurrent.Task]] */
  val flatten = new (FilesystemTask ~> Task) {
    def apply[A](t: FilesystemTask[A]): Task[A] =
      t.fold(e => Task.fail(new RuntimeException(e.shows)), Task.now).join
  }

  def formatAsHttpResponse[S[_]: Functor,A: EntityEncoder](f: S ~> Task)(data: Process[FileSystemErrT[Free[S,?], ?], A],
                                                                         contentType: `Content-Type`,
                                                                         disposition: Option[`Content-Disposition`]): Task[Response] = {
    // Check the first element of data, if it's an error return an error response, otherwise serialize any
    // other errors among the remaining data that is sent to the client.
    convert(f)(data).unconsOption.fold(
      fileSystemErrorResponse,
      _.fold(Ok(""))({ case (first, rest) =>
        Ok(Process.emit(first) ++ rest.translate(flatten))
      }).map(_.putHeaders(contentType :: disposition.toList : _*))).join
  }

  def formatQuasarDataStreamAsHttpResponse[S[_]: Functor](f: S ~> Task)(data: Process[FileSystemErrT[Free[S,?], ?], Data],
                                                                        format: MessageFormat): Task[Response] = {
    formatAsHttpResponse(f)(
      data = format.encode[FileSystemErrT[Free[S,?], ?]](data),
      contentType = `Content-Type`(format.mediaType, Some(Charset.`UTF-8`)),
      disposition = format.disposition
    )
  }

  def convert[S[_]: Functor, A](f: S ~> Task)(from: Process[FileSystemErrT[Free[S,?],?], A]): Process[FilesystemTask, A] = {
    type F[A] = Free[S,A]
    type M[A] = FileSystemErrT[F,A]
    val trans: F ~> Task = hoistFree(f)
    val trans2: M ~> FilesystemTask = Hoist[FileSystemErrT].hoist(trans)
    from.translate(trans2)
  }
}
