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

package quasar.qscript

import quasar.Predef._
import quasar.{UnaryFunc, BinaryFunc, TernaryFunc, Mapping}
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.std._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._
import shapeless.Sized

/** The operations needed to execute the various StdLib tests for a QScript backend. */
trait MapFuncStdLibTestRunner extends StdLibTestRunner {
  /** Translate to MapFunc (common to all QScript backends). */
  def translate[A](prg: Fix[LP], args: Symbol => A): Free[MapFunc[Fix, ?], A] =
    prg.cata[Free[MapFunc[Fix, ?], A]] {
      case lp.InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1))
          if func.effect ≟ Mapping =>
        Free.roll(MapFunc.translateUnaryMapping(func)(a1))

      case lp.InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2))
          if func.effect ≟ Mapping =>
        Free.roll(MapFunc.translateBinaryMapping(func)(a1, a2))

      case lp.InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3))
          if func.effect ≟ Mapping =>
        Free.roll(MapFunc.translateTernaryMapping(func)(a1, a2, a3))

      case lp.Free(sym) => Free.pure(args(sym))
    }
}
