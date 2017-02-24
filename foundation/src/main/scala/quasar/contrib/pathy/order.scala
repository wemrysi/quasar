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

package quasar.contrib.pathy

import quasar.Predef._

import pathy.Path, Path._
import scalaz._, Scalaz._

// FIXME: Remove once we're able to upgrade to pathy-0.2.7+
object order {
  implicit val dirNameOrder: Order[DirName] =
    Order.orderBy(_.value)

  implicit val fileNameOrder: Order[FileName] =
    Order.orderBy(_.value)

  implicit def pathOrder[B,T,S]: Order[Path[B,T,S]] =
    Order.orderBy(p =>
      flatten[(Option[Int], Option[String \/ String])](
        root       =      (some(0),          none),
        parentDir  =      (some(1),          none),
        currentDir =      (some(2),          none),
        dirName    = s => (none   , some( s.left)),
        fileName   = s => (none   , some(s.right)),
        path       = p))
}
