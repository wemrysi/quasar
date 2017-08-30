/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.rdbms

import pathy.Path.posixCodec
import quasar.connector.EnvironmentError
import quasar.contrib.pathy.{unsafeSandboxAbs, ADir}
import quasar.fs.mount.BackendDef.DefErrT
import quasar.fs.mount.ConnectionUri
import quasar.physical.rdbms.common.Config
import slamdata.Predef._

import doobie.hikari.hikaritransactor.HikariTransactor
import scalaz.concurrent.Task
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.{EitherT, NonEmptyList}

package object config {

  def parseUri(uri: ConnectionUri): DefErrT[Task, Config] = {
    final case class JdbcConnectionInfo(
        driverClassName: String,
        url: String,
        userName: String,
        password: String)

    def error[A](msg: String): DefErrT[Task, A] =
      EitherT(NonEmptyList(msg).left[EnvironmentError].left[A].point[Task])

    // extract username and password from connectionuri
    // hardcoded transactor
    def forge(rootPath: String): DefErrT[Task, JdbcConnectionInfo] =
      posixCodec
        .parseAbsDir(rootPath)
        .map(unsafeSandboxAbs _)
        .cata(
          (prefix: ADir) => {
            JdbcConnectionInfo("driver", "uri", "user", "pass")
              .point[DefErrT[Task, ?]] // TODO finish parsing
          },
          error[JdbcConnectionInfo](s"Could not extract a path from $rootPath")
        )

    forge(uri.value).flatMap { connectionInfo =>
      val xa = HikariTransactor[Task](
        "org.postgresql.Driver",
        "jdbc:postgresql://localhost:5432/metastore",
        "postgres",
        ""
      )
      xa.map(common.Config.apply).liftM[DefErrT]
    }
  }
}
