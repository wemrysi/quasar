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

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Offset extends OptionalValidatingQueryParamDecoderMatcher[Natural]("offset")
  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Limit  extends OptionalValidatingQueryParamDecoderMatcher[Positive]("limit")

  private val DestinationHeaderMustExist = BadRequest("The '" + Destination.name + "' header must be specified")

  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S],
                                                    Q: QueryFile.Ops[S]): HttpService = {

    HttpService {
      case req @ GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) => {
        val offsetWithDefault = offsetParam.getOrElse(Natural._0.successNel)
        val offsetWithErrorMsg: String \/ Natural = offsetWithDefault.disjunction.leftMap(
          nel => s"invalid offset: ${nel.head.sanitized} (${nel.head.details})")
        val limitWithErrorMsg: String \/ Option[Positive] = limitParam.traverseU(_.disjunction.leftMap(
          nel => s"invalid limit: ${nel.head.sanitized} (${nel.head.details})"))
        val possibleResponse = (offsetWithErrorMsg |@| limitWithErrorMsg){(offset, limit) =>
          val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
          path.fold(
            dirPath => formatAsHttpResponse(f)(
              data = zippedBytes[S](dirPath, requestedFormat, offset, limit),
              contentType = `Content-Type`(MediaType.`application/zip`),
              disposition = requestedFormat.disposition
            ),
            filePath => formatAsHttpResponse(f)(
              data = requestedFormat.encode(R.scan(filePath, offset, limit)),
              contentType = `Content-Type`(requestedFormat.mediaType, Some(Charset.`UTF-8`)),
              disposition = requestedFormat.disposition
            )
          )
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

  private def responseForUpload[A](decodeErrors: List[WriteError], persistErrors: FilesystemTask[List[FileSystemError]]): Task[Response] = {
    def dataErrorBody[A: Show](status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[A]) =
      status(Json(
        "error" := "some uploaded value(s) could not be processed",
        "details" := errs.map(e => Json("detail" := e.shows))))

    if (decodeErrors.nonEmpty)
      dataErrorBody(BadRequest, decodeErrors)
    else
      persistErrors.run.flatMap(_.fold(
        fileSystemErrorResponse,
        errors => if(errors.isEmpty) Ok("") else dataErrorBody(InternalServerError, errors)))
  }

  implicit val dataDecoder: EntityDecoder[(List[WriteError], List[Data])] =
    MessageFormat.decoder orElse EntityDecoder.error(MessageFormat.UnsupportedContentType)
}
