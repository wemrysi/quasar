package quasar.api.services

import java.nio.charset.StandardCharsets

import quasar.fp._

import argonaut.Json
import argonaut.Argonaut._
import org.http4s._
import org.http4s.dsl._
import org.http4s.argonaut._
import org.http4s.headers.{`Content-Type`, Accept}
import org.http4s.server._
import pathy.Path._
import quasar.fs.ManageFile.{MoveSemantics, MoveScenario}
import quasar.{DataCodec, Data}
import quasar.repl.Prettify
import quasar.Predef._
import quasar.api._
import quasar.fs._
import scodec.bits.ByteVector

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.TraverseOps
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.syntax.show._

import posixCodec._

object data {

  private val DestinationHeaderMustExist = BadRequest("The '" + Destination.name + "' header must be specified")

  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S],
                                                    Q: QueryFile.Ops[S]): HttpService = {

    def download(format: MessageFormat, path: APath, offset: Natural, limit: Option[Positive]) = {
      refineType(path).fold(
        dirPath => formatAsHttpResponse(f)(
          data = zippedBytes[S](dirPath, format, offset, limit),
          contentType = `Content-Type`(MediaType.`application/zip`),
          disposition = format.disposition
        ),
        filePath => formatQuasarDataStreamAsHttpResponse(f)(
          data = R.scan(filePath, offset, limit),
          format = format
        )
      )
    }

    def upload(req: Request,
               by: Process[Free[S,?], Data] => Process[FileSystemErrT[Free[S,?],?], FileSystemError]) = {
      handleMissingContentType(
        req.decode[Process[Task,DecodeError \/ Data]] { data =>
          for {
            all <- data.runLog
            result <- if (all.isEmpty) BadRequest("Request has no body")
            else {
              val (errors, cleanData) = unzipDisj(all.toList)
              // Does the fact that save take a Process[Free, A] doom us to non-streaming upload?
              responseForUpload(errors, convert[S, FileSystemError](f)(by(Process.emitAll(cleanData))).runLog.map(_.toList))
            }
          } yield result
        }
      )
    }

    HttpService {
      case req @ GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) => {
        handleOffsetLimitParams(offsetParam,limitParam){ (offset, limit) =>
          val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
          download(requestedFormat, path, offset.getOrElse(Natural._0), limit)
        }
      }
      case req @ POST -> AsFilePath(path) => upload(req, W.append(path,_))
      case req @ PUT -> AsFilePath(path) => upload(req, W.save(path,_))
      case req @ Method.MOVE -> AsPath(path) =>
        req.headers.get(Destination).fold(
          DestinationHeaderMustExist)(
          destPathString => {
            val scenarioOrProblem = refineType(path).fold(
              src => parseAbsDir(destPathString.value).flatMap(sandbox(rootDir, _)).map(rootDir </> _).map(
                dest => MoveScenario.DirToDir(src, dest)) \/> "Cannot move directory into a file",
              src => parseAbsFile(destPathString.value).flatMap(sandbox(rootDir, _)).map(rootDir </> _).map(
                dest => MoveScenario.FileToFile(src, dest)) \/> "Cannot move a file into a directory, must specify destination precisely"
            )
            scenarioOrProblem.map{ scenario =>
              val response = M.move(scenario, MoveSemantics.FailIfExists).fold(fileSystemErrorResponse,_ => Created(""))
              hoistFree(f).apply(response).join
            }.leftMap(BadRequest(_)).merge
          }
        )
      case DELETE -> AsPath(path) =>
        val response = M.delete(path).fold(fileSystemErrorResponse, _ => Ok(""))
        hoistFree(f).apply(response).join
    }
  }

  def zippedBytes[S[_]: Functor](dir: AbsDir[Sandboxed], format: MessageFormat, offset: Natural, limit: Option[Positive])
                 (implicit R: ReadFile.Ops[S], Q: QueryFile.Ops[S]): Process[R.M, ByteVector] = {
    Process.await(Q.descendantFiles(dir)) { files =>
      val filesAndBytes = files.toList.map { file =>
        val data = R.scan(dir </> file, offset, limit)
        val bytes = format.encode(data).map(str => ByteVector.view(str.getBytes(StandardCharsets.UTF_8)))
        (file, bytes)
      }
      quasar.api.Zip.zipFiles(filesAndBytes)
    }
  }

  def handleMissingContentType(response: Task[Response]) =
    response.handleWith{ case MessageFormat.UnsupportedContentType =>
      UnsupportedMediaType("No media-type is specified in Content-Type header")
        .withContentType(Some(`Content-Type`(MediaType.`text/plain`)))
    }

  private def responseForUpload[A](decodeErrors: List[DecodeError], persistErrors: FilesystemTask[List[FileSystemError]]): Task[Response] = {
    def dataErrorBody[A: Show](status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[A]) =
      status(Json(
        "error" := "some uploaded value(s) could not be processed",
        "details" := Json.array(errs.map(e => jString(e.shows)): _*)))

    if (decodeErrors.nonEmpty)
      dataErrorBody(BadRequest, decodeErrors)
    else
      persistErrors.run.flatMap(_.fold(
        fileSystemErrorResponse,
        errors => if(errors.isEmpty) Ok("") else dataErrorBody(InternalServerError, errors)))
  }

  implicit val dataDecoder: EntityDecoder[Process[Task,DecodeError \/ Data]] =
    MessageFormat.decoder orElse EntityDecoder.error(MessageFormat.UnsupportedContentType)
}
