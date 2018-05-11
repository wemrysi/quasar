/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir

import quasar.Data
import quasar.contrib.pathy.{ADir, AFile, PathSegment}
import quasar.fs.FileSystemType
import quasar.fs.mount.ConnectionUri

import fs2.Stream
import scalaz.EitherT
import scalaz.Scalaz._
import scalaz.concurrent.Task

object MimirFileSystem extends LightweightFileSystem {
  def children(dir: ADir): Task[Option[Set[PathSegment]]] =
    None.point[Task]

  def exists(file: AFile): Task[Boolean] =
    false.point[Task]

  def read(file: AFile): Task[Option[Stream[Task, Data]]] =
    None.point[Task]
}

object MimirLightweight extends LightweightConnector {
  def init(uri: ConnectionUri): EitherT[Task, String, (LightweightFileSystem, Task[Unit])] =
    EitherT.rightT((MimirFileSystem, ().point[Task]).point[Task])
}

object Mimir extends SlamEngine {
  val Type: FileSystemType = FileSystemType("mimir")
  val lwc: LightweightConnector = MimirLightweight
}
