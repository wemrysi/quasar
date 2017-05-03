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
package security

import quasar.blueeyes._
import service.v1
import accounts.AccountId
import accounts.AccountFinder

import scalaz._, Scalaz._

object PermissionsFinder {
  import Permission._
  def canWriteAs(permissions: Set[WritePermission], authorities: Authorities): Boolean = {
    val permWriteAs = permissions.map(_.writeAs)
    permWriteAs.exists(_ == WriteAsAny) || {
      val writeAsAlls = permWriteAs.collect({ case WriteAsAll(s) if s.subsetOf(authorities.accountIds) => s })

      permWriteAs.nonEmpty &&
      writeAsAlls.foldLeft(authorities.accountIds)({ case (remaining, s) => remaining diff s }).isEmpty
    }
  }
}

class PermissionsFinder[M[+ _]: Monad](val apiKeyFinder: APIKeyFinder[M], val accountFinder: AccountFinder[M], timestampRequiredAfter: Instant)
    extends org.slf4s.Logging {
  import PermissionsFinder._
  import Permission._

  private def filterWritePermissions(keyDetails: v1.APIKeyDetails, path: Path, at: Option[Instant]): Set[WritePermission] = {
    keyDetails.grants filter { g =>
      (at exists { g.isValidAt _ }) || g.createdAt.isBefore(timestampRequiredAfter)
    } flatMap {
      _.permissions collect {
        case perm @ WritePermission(path0, _) if path0.isEqualOrParentOf(path) => perm
      }
    }
  }

  def writePermissions(apiKey: APIKey, path: Path, at: Instant): M[Set[WritePermission]] = {
    apiKeyFinder.findAPIKey(apiKey, None) map {
      case Some(details) =>
        log.debug("Filtering write grants from " + details + " for " + path + " at " + at)
        filterWritePermissions(details, path, Some(at))

      case None =>
        log.warn("No API key details found for %s %s at %s".format(apiKey, path.path, at.toString))
        Set()
    }
  }

  def findBrowsableChildren(apiKey: APIKey, path: Path): M[Set[Path]] = {
    for {
      permissions <- apiKeyFinder.findAPIKey(apiKey, None) map { details =>
                      details.toSet.flatMap(_.grants).flatMap(_.permissions)
                    }
      accountId <- accountFinder.findAccountByAPIKey(apiKey)
      accountPath <- accountId.traverse(accountFinder.findAccountDetailsById)
    } yield {
      // FIXME: Not comprehensive/exhaustive in terms of finding all possible data you could read
      permissions flatMap {
        case perm @ WrittenByPermission(p0, _) if p0.isEqualOrParentOf(path) =>
          if (perm.path == Path.Root) accountPath.flatten.map(_.rootPath) else Some(perm.path)

        case _ => None
      }
    }
  }
}
