package quasar
package api
package services

import Predef._

import argonaut._

import org.http4s.server.HttpService
import org.http4s._
import org.scalacheck.Arbitrary
import org.specs2.ScalaCheck
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

import pathy.Path._
import pathy.scalacheck.AbsFileOf
import pathy.scalacheck.PathyArbitrary._
import quasar.fs._
import quasar.fs.InMemory._

import Fixture._
import quasar.std.IdentityLib

import scalaz.concurrent.Task
import scalaz.stream.Process

import query._

class CompileAndQueryServiceSpec extends Specification with FileSystemFixture with ScalaCheck {

  // Remove if eventually included in upstream scala-pathy
  implicit val arbitraryFileName: Arbitrary[FileName] =
    Arbitrary(Arbitrary.arbitrary[AFile].map(fileName(_)))

  import posixCodec.printPath

  def compileService(state: InMemState): HttpService =
    services.query.compileService[FileSystem](runStatefully(state).run.compose(fileSystem))
  def queryService(state: InMemState): HttpService = service[FileSystem](runStatefully(state).run.compose(fileSystem))

  def post[A:EntityDecoder](service: InMemState => HttpService)(path: ADir,
            query: Option[String],
            destination: Option[AFile],
            state: InMemState,
            status: Status,
            response: A => MatchResult[scala.Any]) = {
    val baseReq = Request(uri = Uri(path = printPath(path)), method = Method.POST)
    val reqWithQuery = query.map(query => baseReq.withBody(query).run).getOrElse(baseReq)
    val req = destination.map(destination =>
      reqWithQuery.copy(headers = Headers(Header("Destination", printPath(destination))))
    ).getOrElse(reqWithQuery)
    val actualResponse = service(state)(req).run
    response(actualResponse.as[A].run)
    actualResponse.status must_== status
  }

  def get(service: InMemState => HttpService)(path: ADir,
            query: Option[String],
            state: InMemState,
            status: Status,
            response: String) = {
    val baseReq = Request(uri = Uri(path = printPath(path)))
    val req = query.map(query => baseReq.copy(uri = baseReq.uri.+?("q", query))).getOrElse(baseReq)
    val actualResponse = service(state)(req).run
    actualResponse.as[String].run must_== response
    actualResponse.status must_== status
  }

  def selectAllLP(file: AFile) = LogicalPlan.Invoke(IdentityLib.Squash,List(LogicalPlan.Read(quasar.fs.convert(file))))
  def selectAll(from: FileName) = "select * from \"" + from.value + "\""

  "Compile and Query Service" should {
    def testBoth[A](test: (InMemState => HttpService) => Unit) = {
      "Compile" should {
        test(compileService)
      }
      "Query" should {
        test(queryService)
      }
    }
    testBoth { service =>
      "GET" >> {
        "be 404 for missing directory" ! prop { (dir: ADir, filename: FileName) =>
          get(service)(
            path = dir,
            query = Some(selectAll(filename)),
            state = InMemState.empty,
            status = Status.NotFound,
            response = "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 for missing query" ! prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = "The request must contain a query"
          )
        }
        "be 400 for query error" ! prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = Some("select date where"),
            state = filesystem.state,
            status = Status.BadRequest,
            response = "keyword 'case' expected; `where'"
          )
        }
      }

      () // TODO: Remove after upgrading to specs2 3.x
    }
  }

  "Query" should {
    "execute a simple query" >> {
      "GET" ! prop { filesystem: SingleFileMemState =>
        val query = selectAll(filesystem.filename)
        get(queryService)(
          path = filesystem.parent,
          query = Some(query),
          state = filesystem.state.copy(queryResps = Map(selectAllLP(filesystem.file) -> filesystem.contents)),
          status = Status.Ok,
          response = jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.run.mkString("")
        )
      }
      "POST" ! prop { (filesystem: SingleFileMemState, destination: AbsFileOf[AlphaCharacters]) =>
        post[Json](queryService)(
          path = filesystem.parent,
          query = Some(selectAll(filesystem.filename)),
          destination = Some(destination.path),
          state = filesystem.state.copy(queryResps = Map(selectAllLP(filesystem.file) -> filesystem.contents)),
          status = Status.Ok,
          response = json => json.field("out").flatMap(_.string).map(_ must_== printPath(destination.path))
            .getOrElse(ko("Missing out field on json response"))
        )
      }
      "POST (error conditions)" >> {
        "be 404 for missing directory" ! prop { (dir: ADir, destination: AFile, filename: FileName) =>
          post[String](queryService)(
            path = dir,
            query = Some(selectAll(filename)),
            destination = Some(destination),
            state = InMemState.empty,
            status = Status.NotFound,
            response = _ must_== "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 with missing query" ! prop { (filesystem: SingleFileMemState, destination: AFile) =>
          post[String](queryService)(
            path = filesystem.parent,
            query = None,
            destination = Some(destination),
            state = filesystem.state,
            status = Status.BadRequest,
            response = _ must_== "The body of the POST must contain a query"
          )
        }
        "be 400 with missing Destination header" ! prop { filesystem: SingleFileMemState =>
          post[String](queryService)(
            path = filesystem.parent,
            query = Some(selectAll(filesystem.filename)),
            destination = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = _ must_== "The 'Destination' header must be specified"
          )
        }
        "be 400 for query error" ! prop { (filesystem: SingleFileMemState, destination: AFile) =>
          post[String](queryService)(
            path = filesystem.parent,
            query = Some("select date where"),
            destination = Some(destination),
            state = filesystem.state,
            status = Status.BadRequest,
            response = _ must_== "keyword 'case' expected; `where'"
          )
        }
      }
    }
  }
  "Compile" should {
    "plan simple query" ! prop { filesystem: SingleFileMemState =>
      // Representation of the directory as a string without the leading slash
      val pathString = printPath(filesystem.file).drop(1)
      get(compileService)(
        path = filesystem.parent,
        query = Some(selectAll(filesystem.filename)),
        state = filesystem.state,
        status = Status.Ok,
        response = "Lookup in Memory\nLookup Squash(Read(Path(\"" + pathString + "\"))) in Map()"
      )
    }
  }
}
