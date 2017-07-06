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

package quasar.main.api

import slamdata.Predef.{ -> => _, _ }
import quasar.contrib.pathy.APath
import quasar.fs.mount.{Mounting, MountConfig, MountingFailure, PathMismatchFailure}

import pathy.Path
import pathy.Path._
import scalaz._

trait MountApi {

  def getMount[S[_]](path: APath)(implicit M: Mounting.Ops[S]): OptionT[Free[S, ?], MountConfig] =
    M.lookupConfig(path)

  def moveMount[S[_], T](src: Path[Abs,T,Sandboxed], dst: Path[Abs,T,Sandboxed])(implicit
    M: Mounting.Ops[S],
    S0: MountingFailure :<: S
  ): Free[S, Unit] =
    M.remount[T](src, dst)

  def mount[S[_]](path: APath, mountConfig: MountConfig, replaceIfExists: Boolean)(implicit
    M: Mounting.Ops[S],
    S0: MountingFailure :<: S,
    S1: PathMismatchFailure :<: S
  ): Free[S, Unit] =
    for {
      exists <- M.lookupType(path).isDefined
      mnt = if (replaceIfExists && exists) M.replace(path, mountConfig)
      else M.mount(path, mountConfig)
      _ <- mnt
    } yield ()
}

object MountApi extends MountApi
