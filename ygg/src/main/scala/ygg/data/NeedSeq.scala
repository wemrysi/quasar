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

package ygg.data

import ygg.common._
import scalaz._ // , Scalaz._

/** A vector of individually lazily evaluated elements.
 */
final case class NeedVec[A](xs: Vector[Need[A]]) {
  // def isEvaluatedAt
}

final case class NeedStream[A](run: StreamT[Need, A]) {
  def toStream: Stream[A] = run.toStream.value
  def force(): Vector[A]  = toStream.toVector
}
