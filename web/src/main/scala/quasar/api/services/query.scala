package quasar.api.services

import org.http4s.util.CaseInsensitiveString
import org.http4s._
import org.http4s.dsl._
import org.http4s.server._
import pathy.Path.{AbsDir, Sandboxed}
import quasar.Predef._
import quasar.api.{Destination, AsDirPath}
import quasar.fs
import quasar.fs.{ManageFile, WriteFile, ReadFile}
import quasar.sql.{ParsingPathError, ParsingError, SQLParser, Query}

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object query {

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Q extends QueryParamDecoderMatcher[Query]("q")

  def formatParsingError(error: ParsingError): Task[Response] = error match {
    case ParsingPathError(e) => ???
    case _ => BadRequest(error.message)
  }

  private val QueryParameterMustContainQuery = BadRequest("The request must contain a query")
  private val POSTContentMustContainQuery    = BadRequest("The body of the POST must contain a query")
  private val DestinationHeaderMustExist     = BadRequest("The '" + Destination.name + "' header must be specified")

  def convert(path: AbsDir[Sandboxed]): fs.Path = ???

  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S]): HttpService = {
    HttpService {
      case req @ GET -> AsDirPath(path) :? Q(query) => {

        SQLParser.parseInContext(query, convert(path)).fold(
          formatParsingError,
          expr => ???
        )
      }
      case GET -> _ => QueryParameterMustContainQuery
      case req @ POST -> AsDirPath(path) =>
        EntityDecoder.decodeString(req).flatMap { query =>
          if (query == "") POSTContentMustContainQuery
          else {
            req.headers.get(Destination).fold(DestinationHeaderMustExist) { destination =>
              val parseRes = SQLParser.parseInContext(Query(query),convert(path)).leftMap(formatParsingError)
              val pathRes = ??? //Path(destination.value).from(convert(path)).leftMap(formatParsingError)

              ???
            }
          }
        }
    }
  }

  def compileService[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                           W: WriteFile.Ops[S],
                                           M: ManageFile.Ops[S]): HttpService = {
    HttpService{
      case GET -> AsDirPath(path) :? QueryParam(query) => {
//        for {
//          expr <- SQLParser.parseInContext(query, convert(path)).leftMap(formatParsingError)
//        } yield ???
        ???
      }
      case GET -> _ => QueryParameterMustContainQuery
    }
  }
}
