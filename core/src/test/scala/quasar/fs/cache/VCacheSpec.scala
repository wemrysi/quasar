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

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.KeyValueStoreSpec
import quasar.fs.cache.ViewCacheArbitrary._
import quasar.metastore._

import doobie.imports._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

abstract class VCacheSpec extends KeyValueStoreSpec[AFile, ViewCache] with MetaStoreFixture {
  def eval[A](program: Free[S, A]): A =
    program.foldMap(VCache.interp).transact(transactor).unsafePerformSync
}

class VCacheH2Spec extends VCacheSpec with H2MetaStoreFixture {
  val schema: quasar.db.Schema[Int] = quasar.metastore.Schema.schema
}
