/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.impl._
import quasar.fs.FileSystemError.pathErr
import quasar.fs.PathError.pathNotFound
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.physical.marklogic._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {

  def interpret[S[_]](
    chunkSize: Positive
  )(
    implicit
    S0:    Task :<: S,
    S1:    ClientR :<: S,
    state: KeyValueStore.Ops[ReadFile.ReadHandle, ReadStream[Task], S],
    seq:   MonotonicSeq.Ops[S]
  ): ReadFile ~> Free[S,?] =
    readFromProcess { (file, readOpts) =>
      val dirPath = fileParent(file) </> dir(fileName(file).value)
      Client.exists(dirPath).ifM(
        // Do not remove call to `getItem`. It will still compile and do the wrong thing.
        // This is due to a very shady pattern that was used in marklogic xcc java driver where
        // ResultItem extends XdmItem in order to "forward calls" but that messes up
        // pattern matching, we want the "actual" XdmItem
        Client.readDirectory(dirPath) map { p =>
          val ltd = p.map(item => xcc.xdmitem.toData(item.getItem))
                     .drop(readOpts.offset.get.toInt)

          readOpts.limit.fold(ltd)(l => ltd.take(l.get.toInt))
            .chunk(chunkSize.get.toInt)
            .map(_.right[FileSystemError])
            .right[FileSystemError]
        },
        pathErr(pathNotFound(file)).left[ReadStream[Task]].pure[Free[S, ?]])
    }
}
