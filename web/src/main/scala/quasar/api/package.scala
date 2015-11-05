/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.fs._

import argonaut.{DecodeResult => _, _}
import Argonaut._

import java.io.File

import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.server._
import org.http4s.server.staticcontent._
import org.http4s.util._

import scalaz._, Scalaz._
import scalaz.concurrent._
import pathy.Path._

package object api {

  object Destination extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("Destination")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name == name) Some(header)
      else None
    }
  }

  object FileName extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("X-File-Name")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name == name) Some(header)
      else None
    }
  }

  object HeaderParam extends HttpMiddleware {
    type HeaderValues = Map[CaseInsensitiveString, List[String]]

    def parse(param: String): String \/ HeaderValues = {
      def strings(json: Json): String \/ List[String] =
        json.string.map(str => \/-(str :: Nil)).getOrElse(
          json.array.map { vs =>
            vs.map(v => v.string \/> ("expected string in array; found: " + v.toString)).sequenceU
          }.getOrElse(-\/("expected a string or array of strings; found: " + json)))

      for {
        json <- Parse.parse(param).leftMap("parse error (" + _ + ")")
        obj <- json.obj \/> ("expected a JSON object; found: " + json.toString)
        values <- obj.toList.map { case (k, v) =>
          strings(v).map(CaseInsensitiveString(k) -> _)
        }.sequenceU
      } yield Map(values: _*)
    }

    def rewrite(headers: Headers, param: HeaderValues): Headers =
      Headers(
        param.toList.flatMap {
          case (k, vs) => vs.map(v => Header.Raw(CaseInsensitiveString(k), v))
        } ++
        headers.toList.filterNot(h => param contains h.name))

    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        (req.params.get("request-headers").fold[String \/ Request](\/-(req)) { v =>
          parse(v).map(hv => req.copy(headers = rewrite(req.headers, hv)))
        }).fold(
          err => BadRequest(Json("error" := "invalid request-headers: " + err)),
          service.run)
      }
  }

  object Prefix {
    def apply(prefix: String)(service: HttpService): HttpService = {
      import monocle.macros.GenLens
      import scalaz.std.option._

      val _uri_path = GenLens[Request](_.uri) composeLens GenLens[Uri](_.path)

      val stripChars = prefix match {
        case "/"                    => 0
        case x if x.startsWith("/") => x.length
        case x                      => x.length + 1
      }

      def rewrite(path: String): Option[String] =
        if (path.startsWith(prefix)) Some(path.substring(stripChars))
        else None

      Service.lift { req: Request =>
        _uri_path.modifyF(rewrite)(req) match {
          case Some(req1) => service(req1)
          case None       => HttpService.notFound
        }
      }
    }
  }

  object AsDirPath {
    def unapply(p: HPath): Option[AbsDir[Sandboxed]] = {
      val str = "/" + p.toList.mkString("/")
      posixCodec.parseAbsDir(str) flatMap (sandbox(rootDir, _)) map (rootDir </> _)
    }
  }

  object AsFilePath {
    def unapply(p: HPath): Option[AbsFile[Sandboxed]] = {
      val str = "/" + p.toList.mkString("/")
      posixCodec.parseAbsFile(str) flatMap (sandbox(rootDir, _)) map (rootDir </> _)
    }
  }

  object AsPath {
    def unapply(p: HPath): Option[AbsPath[Sandboxed]] = {
      AsDirPath.unapply(p).map(\/.left) orElse AsFilePath.unapply(p).map(\/.right)
    }
  }

  def staticFileService(basePath: String): HttpService = {
    def pathCollector(file: File, config: FileService.Config, req: Request): Task[Option[Response]] = Task.delay {
      if (file.isDirectory) StaticFile.fromFile(new File(file, "index.html"), Some(req))
      else if (!file.isFile) None
      else StaticFile.fromFile(file, Some(req))
    }

    fileService(FileService.Config(
      systemPath = basePath,
      pathCollector = pathCollector))
  }

  def fileMediaType(file: String): Option[MediaType] =
    MediaType.forExtension(file.split('.').last)

  def redirectService(basePath: String) = HttpService {
    // NB: this means we redirected to a path that wasn't handled, and need
    // to avoid getting into a loop.
    case GET -> path if path.startsWith(HPath(basePath)) => NotFound()

    case GET -> AsPath(path) =>
      TemporaryRedirect(Uri(path = basePath + path.toString))
  }
}
