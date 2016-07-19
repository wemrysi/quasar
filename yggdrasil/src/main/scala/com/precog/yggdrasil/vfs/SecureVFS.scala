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
package com.precog.yggdrasil
package vfs

import blueeyes._
import com.precog.common._, accounts._, ingest._, security._, jobs._
import com.precog.yggdrasil.metadata._
import ResourceError._
import Permission._

import blueeyes.util.Clock

import org.slf4s.Logging

import scalaz._, Scalaz._

trait VFSMetadata[M[+ _]] {
  def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[PathMetadata]]
  def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure]
  def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long]
}

trait SecureVFSModule[M[+ _], Block] extends VFSModule[M, Block] {
  case class StoredQueryResult(data: StreamT[M, Block], cachedAt: Option[Instant], cachingJob: Option[JobId])

  class SecureVFS(vfs: VFS, permissionsFinder: PermissionsFinder[M], jobManager: JobManager[M], clock: Clock)(implicit M: Monad[M])
      extends VFSMetadata[M]
      with Logging {
    final val unsecured = vfs

    private def verifyResourceAccess(apiKey: APIKey, path: Path, readMode: ReadMode): Resource => EitherT[M, ResourceError, Resource] = { resource =>
      log.debug("Verifying access to %s as %s on %s (mode %s)".format(resource, apiKey, path, readMode))
      import AccessMode._
      val permissions: Set[Permission] = resource.authorities.accountIds map { accountId =>
        val writtenBy = WrittenBy(accountId)
        readMode match {
          case Read         => ReadPermission(path, writtenBy)
          case Execute      => ExecutePermission(path, writtenBy)
          case ReadMetadata => ReducePermission(path, writtenBy)
        }
      }

      EitherT {
        permissionsFinder.apiKeyFinder.hasCapability(apiKey, permissions, Some(clock.now())) map {
          case true  => \/.right(resource)
          case false => \/.left(permissionsError("API key %s does not provide %s permission to resource at path %s.".format(apiKey, readMode.name, path.path)))
        }
      }
    }

    final def readResource(apiKey: APIKey, path: Path, version: Version, readMode: ReadMode): EitherT[M, ResourceError, Resource] = {
      vfs.readResource(path, version) >>=
        verifyResourceAccess(apiKey, path, readMode)
    }

    final def readProjection(apiKey: APIKey, path: Path, version: Version, readMode: ReadMode): EitherT[M, ResourceError, Projection] = {
      readResource(apiKey, path, version, readMode) >>=
        Resource.asProjection(path, version)
    }

    final def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long] = {
      readResource(apiKey, path, version, AccessMode.ReadMetadata) flatMap { //need mapM
        _.fold(br => EitherT.right(br.byteLength.point[M]), pr => EitherT.right(pr.recordCount))
      }
    }

    final def pathStructure(apiKey: APIKey, path: Path, selector: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = {
      readProjection(apiKey, path, version, AccessMode.ReadMetadata) >>=
        VFS.pathStructure(selector)
    }

    final def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[PathMetadata]] = path match {
      // If asked for the root path, we simply determine children
      // based on the api key permissions to avoid a massive tree walk
      case Path.Root =>
        log.debug("Defaulting on root-level child browse to account path")
        for {
          children <- EitherT.right(permissionsFinder.findBrowsableChildren(apiKey, path))
          nonRoot = children.filterNot(_ == Path.Root)
          childMetadata <- nonRoot.toList.traverseU(vfs.findPathMetadata)
        } yield {
          childMetadata.toSet
        }

      case other =>
        for {
          children <- vfs.findDirectChildren(path)
          permitted <- EitherT.right(permissionsFinder.findBrowsableChildren(apiKey, path))
        } yield {
          children filter { case PathMetadata(child, _) => permitted.exists(_.isEqualOrParentOf(path / child)) }
        }
    }
  }
}
