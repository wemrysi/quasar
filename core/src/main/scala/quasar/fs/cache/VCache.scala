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

package quasar.fs.cache

import quasar.effect.KeyValueStore
import quasar.metastore._, KeyValueStore._, MetaStoreAccess._

import doobie.imports.ConnectionIO
import scalaz._, Scalaz._

object VCache {
  val interp: VCache ~> ConnectionIO = λ[VCache ~> ConnectionIO] {
    case Keys()                      => Queries.viewCachePaths.list ∘ (_.toVector)
    case Get(k)                      => lookupViewCache(k)
    case Put(k, v)                   => updateInsertViewCache(k, v)
    case CompareAndPut(k, expect, v) =>
      lookupViewCache(k) >>= (vc => (vc ≟ expect).fold(updateInsertViewCache(k, v).as(true), false.η[ConnectionIO]))
    case Delete(k)                   => runOneRowUpdate(Queries.deleteViewCache(k))
  }
}
