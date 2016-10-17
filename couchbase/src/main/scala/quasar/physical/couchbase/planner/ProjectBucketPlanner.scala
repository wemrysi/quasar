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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.contrib.matryoshka.ShowT
import quasar.physical.couchbase._
import quasar.qscript, qscript._

import matryoshka._
import scalaz._

final class ProjectBucketPlanner[F[_]: Monad, T[_[_]]: Recursive: ShowT]
  extends Planner[F, ProjectBucket[T, ?]] {

  def plan: AlgebraM[M, ProjectBucket[T, ?], N1QL] = {
    case BucketField(src, value, name)  => ???
    case BucketIndex(src, value, index) => ???
  }
}
