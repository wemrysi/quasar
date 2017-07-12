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

package quasar.physical.mimir

import slamdata.Predef._

import quasar.Data
import quasar.contrib.scalacheck.gen
import quasar.fp.tree.{BinaryArg, TernaryArg, UnaryArg}
import quasar.qscript._
import quasar.std.StdLibSpec

import matryoshka.data.Fix

import org.scalacheck.{Arbitrary, Gen}
import org.specs2.execute.Result

import java.time.LocalDate

import scalaz.syntax.functor._

class MimirStdLibSpec extends StdLibSpec {
  val runner = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(prg: FreeMapA[Fix, Nothing], expected: Data): Result = ???
    def unaryMapFunc(prg: FreeMapA[Fix, UnaryArg], arg: Data,expected: Data): Result = ???
    def binaryMapFunc(prg: FreeMapA[Fix, BinaryArg], arg1: Data,arg2: Data, expected: Data): Result = ???
    def ternaryMapFunc(prg: FreeMapA[Fix, TernaryArg], arg1: Data,arg2: Data, arg3: Data, expected: Data): Result = ???
    
    def decDomain: Gen[BigDecimal] = Arbitrary.arbitrary[Long].map(BigDecimal(_))
    def intDomain: Gen[BigInt] = Arbitrary.arbitrary[Long].map(BigInt(_))
    def stringDomain: Gen[String] = gen.printableAsciiString

    def dateDomain: Gen[LocalDate] =
      Gen.choose(
        LocalDate.of(1, 1, 1).toEpochDay,
        LocalDate.of(9999, 12, 31).toEpochDay
      ) ∘ (LocalDate.ofEpochDay(_))
  }

  tests(runner)
}
