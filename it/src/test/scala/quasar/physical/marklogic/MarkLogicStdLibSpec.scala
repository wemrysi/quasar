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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.{Data, TestConfig}
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.fp.eitherT._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.qscript._
import quasar.std._

import com.marklogic.xcc.ContentSource
import matryoshka._
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.execute._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MarkLogicStdLibSpec extends StdLibSpec {
  import quasar.frontend.fixpoint.lpf

  def runner(contentSource: ContentSource) = new MapFuncStdLibTestRunner {
    type F[A] = WriterT[State[Long, ?], Prologs, A]
    type G[A] = EitherT[F, MarkLogicPlannerError, A]
    type H[A] = EitherT[F, Result, A]

    def nullary(
      prg: Fix[LP],
      expected: Data): Result = ???

    def unary(
      prg: Fix[LP] => Fix[LP],
      arg: Data,
      expected: Data): Result = {

      val mf = translate[UnaryArg](prg(lpf.free('arg)), κ(UnaryArg._1))

      val xqyPlan = asXqy(arg) flatMap { a1 =>
        planMapFunc[Fix, G, UnaryArg](mf)(κ(a1))
          .leftMap(err => ko(err.shows).toResult)
      }

      run(xqyPlan, expected)
    }

    def binary(
      prg: (Fix[LP], Fix[LP]) => Fix[LP],
      arg1: Data, arg2: Data,
      expected: Data): Result = skipped

    def ternary(
      prg: (Fix[LP], Fix[LP], Fix[LP]) => Fix[LP],
      arg1: Data, arg2: Data, arg3: Data,
      expected: Data): Result = skipped

    def intDomain    = arbitrary[Long]   map (BigInt(_))
    def decDomain    = arbitrary[Double] map (BigDecimal(_))
    def stringDomain = arbitrary[String]

    ////

    private def run(plan: H[XQuery], expected: Data): Result = {
      val (prologs, xqy) = plan.run.run.eval(1)

      val result = xqy.traverse(body => for {
        qr <- SessionIO.evaluateModule_(MainModule(Version.`1.0-ml`, prologs, body))
        rs <- SessionIO.liftT(qr.toImmutableArray)
      } yield {
        rs.headOption
          .flatMap(xdmitem.toData[ErrorMessages \/ ?](_).toOption)
          .fold(ko("No results found."))(_ must beCloseTo(expected))
          .toResult
      })

      runSession(result).unsafePerformSync.merge
    }

    private val runSession: SessionIO ~> Task =
      ContentSourceIO.runNT(contentSource) compose ContentSourceIO.runSessionIO

    private def asXqy(d: Data): H[XQuery] =
      EncodeXQuery[EitherT[F, ErrorMessages, ?], Const[Data, ?]]
        .encodeXQuery(Const(d))
        .leftMap(errs => ko(errs.head).toResult)
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    contentSourceAt(uri).map(cs => backend.name should tests(runner(cs))).void
  }).unsafePerformSync
}
