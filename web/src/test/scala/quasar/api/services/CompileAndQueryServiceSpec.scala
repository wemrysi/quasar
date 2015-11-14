package quasar
package api
package services

import Predef._

import argonaut._, Argonaut._

import org.http4s.server.HttpService
import org.http4s._
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import pathy.Path._
import quasar.fs._
import quasar.fs.inmemory._
import quasar.fs.PathyGen._
import quasar.fs.inmemory.InMemState

import Fixture._

import scalaz.concurrent.Task
import scalaz.stream.Process

import query._

class CompileAndQueryServiceSpec extends Specification with ScalaCheck {

  import posixCodec.printPath

  def compile(state: InMemState): HttpService = compileService[FileSystem](runStatefully(state).run.compose(filesystem))
  def query(state: InMemState): HttpService = service[FileSystem](runStatefully(state).run.compose(filesystem))

  def post(service: InMemState => HttpService)(path: AbsDir[Sandboxed],
            query: Option[String],
            destination: Option[AbsFile[Sandboxed]],
            state: InMemState,
            status: Status,
            response: String) = {
    val baseReq = Request(uri = Uri(path = printPath(path)), method = Method.POST)
    val reqWithQuery = query.map(query => baseReq.withBody(query).run).getOrElse(baseReq)
    val req = destination.map(destination =>
      reqWithQuery.copy(headers = Headers(Header("Destination", printPath(destination))))
    ).getOrElse(reqWithQuery)
    val actualResponse = service(state)(req).run
    actualResponse.status must_== status
    actualResponse.as[String].run must_== response
  }

  def get(service: InMemState => HttpService)(path: AbsDir[Sandboxed],
            query: Option[String],
            state: InMemState,
            status: Status,
            response: String) = {
    val baseReq = Request(uri = Uri(path = printPath(path)))
    val req = query.map(query => baseReq.copy(uri = baseReq.uri.+?("q", query))).getOrElse(baseReq)
    val actualResponse = service(state)(req).run
    actualResponse.status must_== status
    actualResponse.as[String].run must_== response
  }

  def selectAll(from: FileName) = s"select * from ${from.value}"

  "Compile and Query Service" should {
    def testBoth[A](test: (InMemState => HttpService) => Unit) = {
      "Compile" should {
        test(compile)
      }
      "Query" should {
        test(query)
      }
    }
    testBoth { service =>
      "GET" >> {
        "be 404 for missing directory" ! prop { (dir: AbsDir[Sandboxed], filename: FileName) =>
          get(service)(
            path = dir,
            query = Some(selectAll(filename)),
            state = InMemState.empty,
            status = Status.NotFound,
            response = "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 for missing query" ! prop { filesystem: SingleFileFileSystem =>
          get(service)(
            path = filesystem.parent,
            query = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = "The request must contain a query"
          )
        }
        "be 400 for query error" ! prop { filesystem: SingleFileFileSystem =>
          get(service)(
            path = filesystem.parent,
            query = Some("select data where"),
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
      "GET" ! prop { filesystem: SingleFileFileSystem =>
        get(query)(
          path = filesystem.parent,
          query = Some(selectAll(filesystem.filename)),
          state = filesystem.state,
          status = Status.Ok,
          response = jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.run.mkString("")
        )
      }
      "POST" ! prop { (filesystem: SingleFileFileSystem, destination: AbsFile[Sandboxed]) =>
        post(query)(
          path = filesystem.parent,
          query = Some(selectAll(filesystem.filename)),
          destination = Some(destination),
          state = filesystem.state,
          status = Status.Ok,
          response = Json("out" := printPath(destination)).spaces2
        )
      }
      "POST (error conditions)" >> {
        "be 404 for missing directory" ! prop { (dir: AbsDir[Sandboxed], destination: AbsFile[Sandboxed], filename: FileName) =>
          post(query)(
            path = dir,
            query = Some(selectAll(filename)),
            destination = Some(destination),
            state = InMemState.empty,
            status = Status.NotFound,
            response = "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 with missing query" ! prop { (filesystem: SingleFileFileSystem, destination: AbsFile[Sandboxed]) =>
          post(query)(
            path = filesystem.parent,
            query = None,
            destination = Some(destination),
            state = filesystem.state,
            status = Status.BadRequest,
            response = "The body of the POST must contain a query"
          )
        }
        "be 400 with missing Destination header" ! prop { filesystem: SingleFileFileSystem =>
          post(query)(
            path = filesystem.parent,
            query = Some(selectAll(filesystem.filename)),
            destination = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = "The 'Destination' header must be specified"
          )
        }
        "be 400 for query error" ! prop { (filesystem: SingleFileFileSystem, destination: AbsFile[Sandboxed]) =>
          post(query)(
            path = filesystem.parent,
            query = Some("select date where"),
            destination = Some(destination),
            state = filesystem.state,
            status = Status.BadRequest,
            response = "keyword 'case' expected; `where'"
          )
        }
      }
    }
  }
  "Compile" should {
    "plan simple query" ! prop { filesystem: SingleFileFileSystem =>
      get(compile)(
        path = filesystem.parent,
        query = Some(selectAll(filesystem.filename)),
        state = filesystem.state,
        status = Status.Ok,
        response = "Stub\nPlan(logical: Squash(Read(Path(\"bar\"))))"
      )
    }
  }
}
