package quasar
package api
package services

import Predef._

import org.http4s._
import org.http4s.server.HttpService
import org.http4s.dsl._
import org.http4s.util.CaseInsensitiveString
import org.http4s.headers._
import org.specs2.mutable.Specification
import quasar.fs.InMemory._
import Method.MOVE

import scalaz.concurrent.Task

class RestApiSpecs extends Specification {

  def compositeService(serviceMap: Map[String,HttpService]) = HttpService {
    case req =>
      serviceMap.keys.find{req.uri.renderString.startsWith(_)}.map{key =>
        serviceMap(key)(req)
      }.getOrElse(NotFound())
  }

  "OPTIONS" should {
    val restApi = RestApi(Nil,None,8888,_ => Task.now(()))
    val serviceMap = restApi.AllServices(runStatefully(InMemState.empty).run.compose(fileSystem))
    val service = compositeService(serviceMap)

    def testAdvertise(path: String,
                      additionalHeaders: List[Header],
                      resultHeaderKey: HeaderKey.Extractable,
                      expected: List[String]) = {
      val req = Request(
        uri = Uri(path = path),
        method = OPTIONS,
        headers = Headers(Header("Origin", "") :: additionalHeaders))
      val response = service(req).run
      response.headers.get(resultHeaderKey).get.value.split(", ").toList must contain(allOf(expected: _*))
    }

    def advertisesMethodsCorrectly(path: String, expected: List[Method]) =
      testAdvertise(path, Nil, `Access-Control-Allow-Methods`, expected.map(_.name))

    def advertisesHeadersCorrectly(path: String, method: Method, expected: List[HeaderKey]) =
      testAdvertise(
        path = path,
        additionalHeaders = List(Header("Access-Control-Request-Method", method.name)),
        resultHeaderKey = `Access-Control-Allow-Headers`,
        expected = expected.map(_.name.value))

    "advertise GET and POST for /query path" >> advertisesMethodsCorrectly("/query/fs", List(GET, POST))

    "advertise Destination header for /query path and method POST" >> {
      advertisesHeadersCorrectly("/query/fs", POST, List(Destination))
    }

    "advertise GET, PUT, POST, DELETE, and MOVE for /data path" >> {
      advertisesMethodsCorrectly("/data/fs", List(GET, PUT, POST, DELETE, MOVE))
    }
    
    "advertise Destination header for /data path and method MOVE" >> {
      advertisesHeadersCorrectly("/data/fs", MOVE, List(Destination))
    }
  }
}
