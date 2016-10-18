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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar._
import quasar.Planner.PlannerError
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.std._
import quasar.qscript.{MapFunc, MapFuncs}, MapFuncs._
import quasar.contrib.matryoshka._

import matryoshka._, Recursive.ops._
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.execute._
import scalaz._, Scalaz._
import shapeless.Sized

// TODO: pull out MapFunc translation as MapFuncStdLibSpec
class CoreMapStdLibSpec extends StdLibSpec {
  val TODO: Result \/ Unit = Skipped("TODO").left

  /** Identify constructs that are expected not to be implemented. */
  val shortCircuit: AlgebraM[Result \/ ?, MapFunc[Fix, ?], Unit] = {
    case ExtractIsoYear(_) => TODO
    case Power(_, _) => Skipped("TODO: handle large value").left
    case _ => ().right
  }

  /** Translate to MapFunc (common to all QScript backends). */
  def translate[A](prg: Fix[LogicalPlan], args: Symbol => A): Free[MapFunc[Fix, ?], A] =
    prg.cata[Free[MapFunc[Fix, ?], A]] {
      case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
          if func.effect ≟ Mapping =>
        Free.roll(MapFunc.translateUnaryMapping(func)(a1))

      case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
          if func.effect ≟ Mapping =>
        Free.roll(MapFunc.translateBinaryMapping(func)(a1, a2))

      case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
          if func.effect ≟ Mapping =>
        Free.roll(MapFunc.translateTernaryMapping(func)(a1, a2, a3))

      case LogicalPlan.FreeF(sym) => Free.pure(args(sym))
    }

  // TODO: figure out how to pass the args to shortCircuit tso they can be inspected
  def check[A](fm: Free[MapFunc[Fix, ?], A], args: List[Data]): Option[Result] =
    freeCataM(fm)(interpretM[Result \/ ?, MapFunc[Fix, ?], A, Unit](κ(().right), shortCircuit)).swap.toOption

  /** Compile/execute on this backend, and compare with the expected value. */
  // TODO: this signature might not work for other implementations.
  def run[A](fm: Free[MapFunc[Fix, ?], A], args: A => Data, expected: Data): Result = {
    val run = freeCataM(fm)(interpretM[PlannerError \/ ?, MapFunc[Fix, ?], A, Data => Data](
      a => κ(args(a)).right, CoreMap.change))
    (run.map(_(Data.NA)) must beRightDisjunction.like { case d => d must closeTo(expected) }).toResult
  }

  val runner = new StdLibTestRunner {
    def nullary(prg: Fix[LogicalPlan], expected: Data) =
      failure

    def unary(prg: Fix[LogicalPlan] => Fix[LogicalPlan], arg: Data, expected: Data) = {
      val mf = translate(prg(LogicalPlan.Free('arg)), κ(UnaryArg._1))

      check(mf, List(arg)) getOrElse
        run(mf, κ(arg), expected)
    }

    def binary(prg: (Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan], arg1: Data, arg2: Data, expected: Data) = {
      val mf = translate[BinaryArg](prg(LogicalPlan.Free('arg1), LogicalPlan.Free('arg2)), {
        case 'arg1 => BinaryArg._1
        case 'arg2 => BinaryArg._2
      })

      check(mf, List(arg1, arg2)) getOrElse
       run[BinaryArg](mf, _.fold(arg1, arg2), expected)
    }

    def ternary(prg: (Fix[LogicalPlan], Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan], arg1: Data, arg2: Data, arg3: Data, expected: Data) = {
      val mf = translate[TernaryArg](prg(LogicalPlan.Free('arg1), LogicalPlan.Free('arg2), LogicalPlan.Free('arg3)), {
        case 'arg1 => TernaryArg._1
        case 'arg2 => TernaryArg._2
        case 'arg3 => TernaryArg._3
      })

      check(mf, List(arg1, arg2, arg3)) getOrElse
       run[TernaryArg](mf, _.fold(arg1, arg2, arg3), expected)
    }

    def intDomain = arbitrary[BigInt]

    // NB: BigDecimal parsing cannot handle values that are too close to the
    // edges of its range.
    def decDomain = arbitrary[BigDecimal].filter(i => i.scale > Int.MinValue && i.scale < Int.MaxValue)

    def stringDomain = arbitrary[String]
  }

  tests(runner)
}
