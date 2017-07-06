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
import quasar.db.DbConnectionConfig
import quasar.effect.AtomicRef
import quasar.fp.free._
import quasar.main.{initUpdateMigrate, MainTask}
import quasar.main.metastore.metastoreTransactor
import quasar.metastore.MetaStore

import scalaz._, Scalaz._
import scalaz.concurrent.Task

trait MetaStoreApi {

  /** Return the connection info of the current metastore being used */
  def getCurrentMetastore[S[_]](implicit M: AtomicRef.Ops[MetaStore, S]): Free[S, DbConnectionConfig] =
    M.get.map(_.connectionInfo)

  /** Attempt to change the metastore being used
    *
    * @param newConnection
    * @param initialize Whether or not to initialize the metastore if it has not already been initialized
    * @return `Unit` if the change was performed successfully otherwise a `String` explaining what went wrong
    */
  def attemptChangeMetastore[S[_]](newConnection: DbConnectionConfig, initialize: Boolean)(implicit
    M: AtomicRef.Ops[MetaStore, S],
    S0: Task :<: S
  ): Free[S, String \/ Unit] = {
    val tryNewMetaStore = metastoreTransactor(newConnection).flatMap(trans =>
      if (initialize)
        initUpdateMigrate(quasar.metastore.Schema.schema, trans.transactor, None).as(trans)
      else trans.point[MainTask])
    for {
      trans  <- lift(tryNewMetaStore.run).into[S]
      result <- trans.traverse(t => M.set(MetaStore(newConnection, t)))
    } yield result
  }
}

object MetaStoreApi extends MetaStoreApi
