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
import scalaz.syntax.validation._
import scalaz.syntax.traverse._
import scalaz.syntax.TraverseOps
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.syntax.show._
import scalaz.std.option._

import posixCodec._

object data {
  import scalaz.Validation.FlatMap._

  implicit val naturalParamDecoder: org.http4s.QueryParamDecoder[Natural] = new QueryParamDecoder[Natural] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Natural] =
      QueryParamDecoder[Long].decode(value).flatMap(long =>
        Natural(long).toSuccess(NonEmptyList(ParseFailure(value.value, "must be >= 0")))
      )
  }

  implicit val positiveParamDecoder: org.http4s.QueryParamDecoder[Positive] = new QueryParamDecoder[Positive] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Positive] =
      QueryParamDecoder[Long].decode(value).flatMap(long =>
        Positive(long).toSuccess(NonEmptyList(ParseFailure(value.value, "must be >= 1")))
      )
  }

  object Offset extends OptionalValidatingQueryParamDecoderMatcher[Natural]("offset")
  object Limit  extends OptionalValidatingQueryParamDecoderMatcher[Positive]("limit")

  private val DestinationHeaderMustExist = BadRequest("The '" + Destination.name + "' header must be specified")

  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S]): HttpService = {

    def download(format: MessageFormat, path: AbsPath[Sandboxed], offset: Natural, limit: Option[Positive]) = {
      path.fold(
        dirPath => formatAsHttpResponse(f)(
          data = zippedBytes[S](dirPath, format, offset, limit),
          contentType = `Content-Type`(MediaType.`application/zip`)
        ),
        filePath => formatAsHttpResponse(f)(
          data = format.encode(R.scan(filePath, offset, limit)),
          contentType = `Content-Type`(format.mediaType, Some(Charset.`UTF-8`))
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
        val offsetWithDefault = offsetParam.getOrElse(Natural._0.successNel)
        val offsetWithErrorMsg: String \/ Natural = offsetWithDefault.disjunction.leftMap(
          nel => s"invalid offset: ${nel.head.sanitized} (${nel.head.details})")
        val limitWithErrorMsg: String \/ Option[Positive] = limitParam.traverseU(_.disjunction.leftMap(
          nel => s"invalid limit: ${nel.head.sanitized} (${nel.head.details})"))
        val possibleResponse = (offsetWithErrorMsg |@| limitWithErrorMsg) { (offset, limit) =>
          val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
          download(requestedFormat, path, offset, limit).map(_.putHeaders(requestedFormat.disposition.toList: _*))
        }
        possibleResponse.leftMap(errMessage => BadRequest(errMessage)).merge
      }
      case req @ POST -> AsFilePath(path) => upload(req, W.append(path,_))
      case req @ PUT -> AsFilePath(path) => upload(req, W.save(path,_))
      case req @ Method.MOVE -> AsPath(path) =>
        req.headers.get(Destination).fold(
          DestinationHeaderMustExist)(
          destPathString => {
            val scenarioOrProblem = path.fold(
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
                 (implicit R: ReadFile.Ops[S], M: ManageFile.Ops[S]): Process[R.M, ByteVector] = {
    Process.await(M.descendantFiles(dir)) { files =>
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
