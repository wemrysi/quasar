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

package quasar.physical.rdbms.fs.postgres

import quasar.connector.EnvironmentError
import quasar.fs.mount.BackendDef.DefinitionError
import quasar.fs.mount.ConnectionUri
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo
import slamdata.Predef._
import scalaz.{-\/, NonEmptyList, \/, \/-}
import scalaz.syntax.either._

object Postgres extends Rdbms {

  override val Type = FsType

  val driverClass = "org.postgres.Driver"

  val formatHint =
    "jdbc:postgres://host:port/db_name?user=username(&password=pw)"
  val JdbcUriOuter = """(jdbc:postgresql://[^\?]*)(.*)$""".r
  val UserPasswordParams = """^\?user=(.*)&password=([^&]*)(.*)$""".r
  val OnlyUserParams = """^\?user=([^&]*)(.*)$""".r

  private def parsingErr(uri: ConnectionUri) =
    -\/(NonEmptyList(
      s"Cannot extract credentials from URI [${uri.value}]. Expected format: $formatHint"))

  def toDefErr[T](errs: \/[NonEmptyList[String], T]): \/[DefinitionError, T] =
    errs.leftMap(_.left[EnvironmentError])

  override def parseConnectionUri(
      uri: ConnectionUri): \/[DefinitionError, JdbcConnectionInfo] = {
    val connectionInfo = uri.value match {
      case JdbcUriOuter(url, params) =>
        val userPass = params match {
          case UserPasswordParams(u, p, _) => \/-((u, Some(p)))
          case OnlyUserParams(u, _) => \/-((u, None))
          case _ => parsingErr(uri)
        }
        userPass.map {
          case (u, p) => JdbcConnectionInfo(driverClass, url, u, p)
        }
      case _ => parsingErr(uri)
    }

    connectionInfo.leftMap(_.left[EnvironmentError])
  }
}
