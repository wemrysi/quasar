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

package quasar.api.services

import quasar.Predef._

import argonaut._, Argonaut._
import org.http4s._, argonaut._, dsl._
import org.http4s.server.HttpService

import scalaz._, concurrent.Task
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

import pathy.Path._

import quasar.api.AsPath
import quasar.fs._

object metadata {

  def service[S[_]: Functor](f: S ~> Task)(implicit Q: QueryFile.Ops[S]): HttpService = {
    def dirMetadata(d: ADir): Q.F[Task[Response]] =
      Q.ls(d).fold(
          fileSystemErrorResponse,
          nodes => Ok(Json.obj("children" := nodes.toList.sorted)))

    def fileMetadata(f: AFile): Q.F[Task[Response]] =
      Q.fileExists(f).fold(
        fileSystemErrorResponse,
        _ ? Ok(Json.obj()) | NotFound(Json("error" := s"File not found: ${posixCodec.printPath(f)}")))

    HttpService {
      case GET -> AsPath(path) =>
        refineType(path).fold(dirMetadata, fileMetadata).foldMap(f).join
    }
  }
}
