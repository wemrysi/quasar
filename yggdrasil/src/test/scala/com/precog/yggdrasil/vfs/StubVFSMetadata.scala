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

import ResourceError.NotFound
import com.precog.common._, security._, ingest._
import scalaz._, Scalaz._
import PathMetadata._

class VFSMetadata(projectionMetadata: Map[Path, Map[ColumnRef, Long]]) {
  def findDirectChildren(apiKey: APIKey, path: Path): EitherT[Need, ResourceError, Set[PathMetadata]] = EitherT.right {
    Need {
      projectionMetadata.keySet collect {
        case key if key.isChildOf(path) =>
          PathMetadata(
            Path(key.components(path.length)),
            if (key.length == path.length + 1) DataOnly(FileContent.XQuirrelData) else PathOnly
          )
      }
    }
  }
}
