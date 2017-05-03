/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package quasar.precog.common
package accounts

import quasar.precog.common.Path
import quasar.precog.common.security.{ APIKey, Permission, ReadPermission, WritePermission, DeletePermission }
import Permission._

import quasar.blueeyes._, json._, serialization._
import quasar.blueeyes.json.serialization.IsoSerialization._
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.json.serialization.Versioned._

import scalaz.Validation
import scalaz.syntax.apply._
import scalaz.syntax.plus._

case class AccountPlan(planType: String)
object AccountPlan {
  val Root = AccountPlan("Root")
  val Free = AccountPlan("Free")

  val schema                           = "type" :: HNil
  implicit val (decomposer, extractor) = serializationV[AccountPlan](schema, None)
}

case class Account(accountId: AccountId,
                   email: String,
                   passwordHash: String,
                   passwordSalt: String,
                   accountCreationDate: DateTime,
                   apiKey: APIKey,
                   rootPath: Path,
                   plan: AccountPlan,
                   parentId: Option[String] = None,
                   lastPasswordChangeTime: Option[DateTime] = None,
                   profile: Option[JValue] = None)

object Account {
  val schemaV1     = "accountId" :: "email" :: "passwordHash" :: "passwordSalt" :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" :: "parentId" :: "lastPasswordChangeTime" :: "profile" :: HNil

  val extractorPreV             = extractorV[Account](schemaV1, None)
  val extractorV1               = extractorV[Account](schemaV1, Some("1.1".v))
  implicit val accountExtractor = extractorV1 <+> extractorPreV

  implicit val decomposerV1 = decomposerV[Account](schemaV1, Some("1.1".v))

  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut): String
  }

  def newAccountPermissions(accountId: AccountId, accountPath: Path): Set[Permission] = {
    // Path is "/" so that an account may read data it wrote no matter what path it exists under.
    // See AccessControlSpec, NewGrantRequest
    Set[Permission](
      WritePermission(accountPath, WriteAsAny),
      DeletePermission(accountPath, WrittenByAny),
      ReadPermission(Path.Root, WrittenByAccount(accountId))
    )
  }
}

case class WrappedAccountId(accountId: AccountId)

object WrappedAccountId {
  val schema = "accountId" :: HNil

  implicit val (wrappedAccountIdDecomposer, wrappedAccountIdExtractor) = serializationV[WrappedAccountId](schema, None)
}
