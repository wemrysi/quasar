package quasar.api.services

import org.http4s.headers.Accept
import org.http4s.util.CaseInsensitiveString
import org.http4s._
import org.http4s.dsl._
import org.http4s.server._
import pathy.Path.{AbsDir, Sandboxed}
import quasar.Planner.{CompilePathError, CompilationError}
import quasar._
import quasar.Predef._
import quasar.api.{MessageFormat, Destination, AsDirPath}
import quasar.{Variables, fs}
import quasar.fs._
import quasar.sql.{ParsingPathError, ParsingError, SQLParser, Query}

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.~>

object query {

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object QueryParam extends QueryParamDecoderMatcher[Query]("q")

  def formatParsingError(error: ParsingError): Task[Response] = error match {
    case ParsingPathError(e) => ???
    case _ => BadRequest(error.message)
  }

  private val QueryParameterMustContainQuery = BadRequest("The request must contain a query")
  private val POSTContentMustContainQuery    = BadRequest("The body of the POST must contain a query")
  private val DestinationHeaderMustExist     = BadRequest("The '" + Destination.name + "' header must be specified")

  def translateSemanticErrors(error: SemanticErrors): Task[Response] = ???

  def convert(path: AbsDir[Sandboxed]): fs.Path = ???

  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S],
                                                    Q: QueryFile.Ops[S]): HttpService = {

    val removePhaseResults = new (FileSystemErrT[PhaseResultT[Free[S,?], ?], ?] ~> FileSystemErrT[Free[S,?], ?]) {
      def apply[A](t: FileSystemErrT[PhaseResultT[Free[S,?], ?], A]): FileSystemErrT[Free[S,?], A] =
        EitherT[Free[S,?],FileSystemError,A](t.run.value)
    }

    HttpService {
      case req @ GET -> AsDirPath(path) :? QueryParam(query) => {

        SQLParser.parseInContext(query, convert(path)).fold(
          formatParsingError,
          expr => queryPlan(expr, Variables(Map())).run.value.fold(
            errs => translateSemanticErrors(errs),
            logicalPlan => {
              val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
              formatQuasarDataStreamAsHttpResponse(f)(
                data = Q.evaluate(logicalPlan).translate[FileSystemErrT[Free[S,?], ?]](removePhaseResults),
                format = requestedFormat)
            }
          )
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
