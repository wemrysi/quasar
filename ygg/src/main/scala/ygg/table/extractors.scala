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

package ygg.table

import ygg._, common._
import quasar._
import quasar.frontend.{ logicalplan => lp }

object SquashInvoke {
  def unapply[A](x: LP[A]): Option[(UnaryFunc, A)] = x match {
    case lp.InvokeUnapply(func @ UnaryFunc(Squashing, _, _, _, _, _, _), Sized(a1)) => Some((func, a1))
    case _                                                                          => None
  }
}
object UnaryMF {
  def unapply[A](x: LP[A]): Option[(UnaryFunc, A)] = x match {
    case lp.InvokeUnapply(func @ UnaryFunc(Mapping, _, _, _, _, _, _), Sized(a1)) => Some((func, a1))
    case _                                                                        => None
  }
}
object BinaryMF {
  def unapply[A](x: LP[A]): Option[(BinaryFunc, A, A)] = x match {
    case lp.InvokeUnapply(func @ BinaryFunc(Mapping, _, _, _, _, _, _), Sized(a1, a2)) => Some((func, a1, a2))
    case _                                                                             => None
  }
}
object TernaryMF {
  def unapply[A](x: LP[A]): Option[(TernaryFunc, A, A, A)] = x match {
    case lp.InvokeUnapply(func @ TernaryFunc(Mapping, _, _, _, _, _, _), Sized(a1, a2, a3)) => Some((func, a1, a2, a3))
    case _                                                                                  => None
  }
}
