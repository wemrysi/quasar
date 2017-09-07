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

import quasar.contrib.pathy.{AFile, APath}
import quasar.effect.MonotonicSeq
import quasar.fs.{ManageFile, MoveSemantics}
import quasar.physical.rdbms.Rdbms
import slamdata.Predef._
import pathy.Path._
import scalaz._
import Scalaz._

trait RdbmsManageFile {
  this: Rdbms =>

  override def ManageFileModule = new ManageFileModule {

    override def move(scenario: ManageFile.MoveScenario, semantics: MoveSemantics): Backend[Unit] = {
      // TODO
      ().point[Backend]
    }

    override def delete(path: APath): Backend[Unit] = {

      // TODO drop table or drop all tables in a dir
      ().point[Backend]
    }

    override def tempFile(near: APath): Backend[AFile] = {
      MonotonicSeq.Ops[Eff].next.map { i =>
        val tmpFilename = file(s"__quasar_tmp_table_$i")
        refineType(near).fold(
          d => d </> tmpFilename,
          f => fileParent(f) </> tmpFilename)
      }.liftB
    }

  }
}
