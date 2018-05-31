/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.regression

import slamdata.Predef._
import quasar._
import quasar.api.{QueryEvaluator, ResourceDiscovery, ResourceName, ResourcePath}
import quasar.api.ResourceError.ReadError
import quasar.build.BuildInfo
import quasar.common.PhaseResults
import quasar.compile.{queryPlan, SemanticErrors}
import quasar.contrib.argonaut._
import quasar.contrib.fs2.convert
import quasar.contrib.fs2.stream._
import quasar.contrib.pathy._
import quasar.ejson
import quasar.ejson.Common.{Optics => CO}
import quasar.evaluate.FederatingQueryEvaluator
import quasar.fp._
import quasar.fs.Planner.PlannerError
import quasar.frontend.logicalplan.{LogicalPlan => LP, Read => LPRead}
import quasar.higher.HFunctor
import quasar.impl.datasource.local.LocalDataSource
import quasar.mimir.Precog
import quasar.mimir.evaluate.{MimirQueryFederation, QueryAssociate}
import quasar.qscript.QScriptEducated
import quasar.qsu.LPtoQS
import quasar.sql, sql.Sql

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.matching.Regex

import java.math.{MathContext, RoundingMode}
import java.nio.file.{Files, Path => JPath, Paths}
import java.text.ParseException

import argonaut._, Argonaut._
import delorean._
import eu.timepit.refined.auto._
import fs2.{concurrent, io, text, Stream}
import fs2.interop.scalaz._
import matryoshka._
import matryoshka.implicits._
import matryoshka.data.Fix
import org.specs2.execute
import org.specs2.specification.core.Fragment
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class Sql2QueryRegressionSpec extends Qspec {
  import Sql2QueryRegressionSpec._

  type M[A] = EitherT[EitherT[StateT[WriterT[Task, PhaseResults, ?], Long, ?], SemanticErrors, ?], PlannerError, A]

  val taskToM: Task ~> M =
    λ[Task ~> M](
      _.liftM[WriterT[?[_], PhaseResults, ?]]
        .liftM[StateT[?[_], Long, ?]]
        .liftM[EitherT[?[_], SemanticErrors,?]]
        .liftM[EitherT[?[_], PlannerError,?]])

  val queryEvaluator =
    for {
      tmpPath <- Task.delay(Files.createTempDirectory("quasar-test-"))
      tmpDir = tmpPath.toFile

      cake <- Precog(tmpDir)

      local = LocalDataSource[Task, Task](jPath(TestDataRoot), 8192)

      localM = HFunctor[QueryEvaluator[?[_], Stream[Task, ?], ResourcePath, Stream[Task, Data]]].hmap(local)(taskToM)

      sdown =
        cake.shutdown.toTask
          .onFinish(_ => Task.delay(tmpDir.deleteOnExit()))
          .onFinish(_ => local.shutdown)

      mimirFederation = MimirQueryFederation[Fix, M](cake, taskToM)

      disposed = (rp: ResourcePath) => localM.evaluate(rp).map(_.map(_.point[Disposable[Task, ?]]))
      qassoc = QueryAssociate.lightweight[Fix, M, Task](disposed)
      discovery = (localM : ResourceDiscovery[M, Stream[Task, ?]])

      federated = FederatingQueryEvaluator(
        mimirFederation,
        IMap((ResourceName("local"), (discovery, qassoc))).point[M])
    } yield (federated, sdown)

  val DataDir = rootDir[Sandboxed] </> dir("local")

  /** A name to identify the suite in test output. */
  def suiteName: String = "SQL^2 Regression Queries"

  /** Return the results of evaluating the given query as a stream. */
  def queryResults(
      f: Fix[QScriptEducated[Fix, ?]] => M[ReadError \/ Stream[Task, Data]])(
      expr: Fix[Sql],
      vars: Variables,
      basePath: ADir)
      : Stream[Task, Data] = {

    def failS(msg: String): Stream[Task, Data] =
      Stream.fail(new RuntimeException(msg))

    val results =
      for {
        lp <- queryPlan[M, Fix, Fix[LP]](expr, vars, basePath, 0L, None)

        dataPaths = lp.transCata[Fix[LP]] {
          case LPRead(p) => LPRead[Fix[LP]](p <:> "data")
          case other => other
        }

        qs <- LPtoQS[Fix].apply[M](dataPaths)

        r  <- f(qs)
      } yield r.valueOr(e => failS(e.shows))

    Stream.force(
      results
        .valueOr(e => failS(e.shows))
        .valueOr(e => failS(e.shows))
        .eval(0)
        .value)
  }

  ////

  (regressionTests(TestsRoot, TestDataRoot) |@| queryEvaluator)({
    case (tests, (eval, sdown)) =>
      suiteName >> {
        tests.toList foreach { case (f, t) =>
          regressionExample(f, t, BackendName("mimir"), queryResults(eval.evaluate))
        }

        step(sdown.unsafePerformSync)
      }
  }).unsafePerformSync

  ////

  /** Returns an `Example` verifying the given `RegressionTest`. */
  def regressionExample(
      loc: RFile,
      test: RegressionTest,
      backendName: BackendName,
      results: (Fix[Sql], Variables, ADir) => Stream[Task, Data])
      : Fragment = {
    def runTest: execute.Result = {
      val data = testQuery(
        test.data.nonEmpty.fold(DataDir </> fileParent(loc), rootDir),
        test.query,
        test.variables,
        results)

      verifyResults(test.expected, data, backendName)
        .timed(30.seconds)
        .unsafePerformSync
    }

    s"${test.name} [${posixCodec.printPath(loc)}]" >> {
      collectFirstDirective(test.backends, backendName) {
        case TestDirective.Skip    => skipped
        case TestDirective.SkipCI  =>
          BuildInfo.isCIBuild.fold(execute.Skipped("(skipped during CI build)"), runTest)
        case TestDirective.Timeout =>
          // NB: To locally skip tests that time out, make `Skipped` unconditional.
          BuildInfo.isCIBuild.fold(
            execute.Skipped("(skipped because it times out)"),
            runTest)
        case TestDirective.Pending | TestDirective.PendingIgnoreFieldOrder =>
          runTest.pendingUntilFixed
      } getOrElse runTest
    }
  }

  /** Verify the given results according to the provided expectation. */
  def verifyResults(
      exp: ExpectedResult,
      act: Stream[Task, Data],
      backendName: BackendName)
      : Task[execute.Result] = {

    /** This helps us get identical results on different connectors, even though
      * they have different precisions for their floating point values.
      */
    val reducePrecision =
      λ[EndoK[ejson.Common]](CO.dec.modify(_.round(TestContext))(_))

    val normalizeJson: Json => Json =
      j => Recursive[Json, ejson.Json].transCata(j)(liftFF(reducePrecision[Json]))

    val deleteFields: Json => Json =
      _.withObject(exp.ignoredFields.foldLeft(_)(_ - _))

    val fieldOrderSignificance =
      if (exp.ignoreFieldOrder)
        OrderIgnored
      else
        collectFirstDirective[OrderSignificance](exp.backends, backendName) {
          case TestDirective.IgnoreAllOrder |
               TestDirective.IgnoreFieldOrder |
               TestDirective.PendingIgnoreFieldOrder =>
            OrderIgnored
        } | OrderPreserved

    val resultOrderSignificance =
      if (exp.ignoreResultOrder)
        OrderIgnored
      else
        collectFirstDirective[OrderSignificance](exp.backends, backendName) {
          case TestDirective.IgnoreAllOrder |
               TestDirective.IgnoreResultOrder |
               TestDirective.PendingIgnoreFieldOrder =>
            OrderIgnored
        } | OrderPreserved

    val result =
      exp.predicate(
        exp.rows.toVector,
        convert.toProcess(act.map(normalizeJson <<< deleteFields <<< (_.asJson))),
        fieldOrderSignificance,
        resultOrderSignificance)

    collectFirstDirective(exp.backends, backendName) {
      case TestDirective.Timeout =>
        result.map {
          case execute.Success(_, _) =>
            execute.Failure(s"Fixed now, you should remove the “timeout” status.")

          case execute.Failure(m, _, _, _) =>
            execute.Failure(s"Failed with “$m”, you should change the “timeout” status.")

          case x => x
        }.handle {
          case e: java.util.concurrent.TimeoutException =>
            execute.Pending(s"times out: ${e.getMessage}")

          case e =>
            execute.Failure(s"Errored with “${e.getMessage}”, you should change the “timeout” status to “pending”.")
        }
    } getOrElse result.handle {
      case e: java.util.concurrent.TimeoutException =>
        execute.Failure(s"Times out (${e.getMessage}), you should use the “timeout” status.")
    }
  }

  /** Parse and execute the given query, returning a stream of results. */
  def testQuery(
      loc: ADir,
      qry: String,
      vars: Map[String, String],
      results: (Fix[Sql], Variables, ADir) => Stream[Task, Data])
      : Stream[Task, Data] =
    sql.fixParser.parseExpr(qry).fold(
      e => Stream.fail(new ParseException(e.message, -1)),
      s => results(s.mkPathsAbsolute(loc), Variables.fromMap(vars), loc))

  /** Returns all the `RegressionTest`s found in the given directory, keyed by
    * file path.
    */
  def regressionTests(
      testDir: RDir,
      dataDir: RDir)
      : Task[Map[RFile, RegressionTest]] =
    concurrent.join(12)(
      descendantsMatching(testDir, TestPattern)
        .map(f => loadRegressionTest(f).map((f.relativeTo(dataDir).get, _))))
      .runFold(Map[RFile, RegressionTest]())(_ + _)

  /** Loads a `RegressionTest` from the given file. */
  def loadRegressionTest(file: RFile): Stream[Task, RegressionTest] =
    io.file.readAllAsync[Task](jPath(file), 1024)
      .through(text.utf8Decode)
      .reduce(_ + _)
      .flatMap(txt =>
        decodeJson[RegressionTest](txt).fold(
          err => Stream.fail(new RuntimeException(posixCodec.printPath(file) + ": " + err)),
          Stream.emit))

  /** Returns all descendant files in the given dir matching `pattern`. */
  def descendantsMatching(d: RDir, pattern: Regex): Stream[Task, RFile] =
    convert.fromJavaStream(Task.delay(Files.walk(jPath(d))))
      .filter(p => pattern.findFirstIn(p.getFileName.toString).isDefined)
      .map(p => posixCodec.parseRelFile(p.toString).flatMap(_.relativeTo(d)))
      .unNone
      .map(d </> _)

  /** Whether the backend has the specified directive in the given test. */
  def hasDirective(td: TestDirective, dirs: Directives, name: BackendName): Boolean =
    Foldable[Option].compose[NonEmptyList].any(dirs get name)(_ ≟ td)

  /** Returns the result of the given partial function applied to the first directive
    * for which it is defined, or `None` if it is undefined for all directives.
    */
  def collectFirstDirective[A](
      dirs: Directives,
      name: BackendName)(
      pf: PartialFunction[TestDirective, A])
      : Option[A] =
    Foldable[Option].compose[NonEmptyList]
      .findMapM[Id, TestDirective, A](dirs get name)(pf.lift)

  def jPath(p: Path[_, _, Sandboxed]): JPath =
    Paths.get(posixCodec.printPath(p))
}

object Sql2QueryRegressionSpec {
  val TestDataRoot: RDir =
    currentDir[Sandboxed] </> dir("it") </> dir("src") </> dir("main") </> dir("resources") </> dir("tests")

  val TestsRoot: RDir =
    TestDataRoot

  val TestPattern: Regex =
    """^([^.].*)\.test""".r

  val TestContext: MathContext =
    new MathContext(13, RoundingMode.DOWN)

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(DataCodec.Precise.encode(_).getOrElse(jString("Undefined")))
}
