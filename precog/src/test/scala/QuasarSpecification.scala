package quasar

import org.specs2.main
import org.specs2.matcher._
import org.specs2.execute._
import org.specs2.specification.dsl._
import org.scalacheck.Arbitrary
import org.scalacheck.util.{ FreqMap, Pretty }
import org.specs2.scalacheck._
import scalaz._

abstract class SequentialQspec extends Qspec {
  sequential

}
abstract class Qspec extends QuasarSpecification

trait QuasarSpecification
    extends org.specs2.mutable.SpecLike
    with ScalaCheckParameters
    with ScalaCheckPropertyDsl
    with org.specs2.scalaz.ScalazMatchers
    with MatchersImplicits
    with AnyMatchers
    with MapMatchers
    with TraversableMatchers
    with PendingUntilFixed
    with ExampleDsl
    with ValueChecks {
  // Report all test timings.
  args.report(showtimes = main.ArgProperty(true))

  implicit class Specs2ScalazOps[A : Equal : Show](lhs: A) {
    def must_=(rhs: A) = lhs must equal(rhs)
  }

  /** Allows marking non-deterministically failing tests as such,
    *  in the manner of pendingUntilFixed but such that it will not
    *  fail regardless of whether it seems to pass or fail.
    */
  implicit class FlakyTest[T: AsResult](t: => T) {
    import org.specs2.execute._
    def flakyTest: Result = flakyTest("")
    def flakyTest(m: String): Result = ResultExecution.execute(AsResult(t)) match {
      case s: Success => s
      case r =>
        val explain = if (m == "") "" else s" ($m)"
        Skipped(s"${ r.message }, but test is marked as flaky$explain", r.expected)
    }
  }

  /** create a ScalaCheck property from a function */
  def prop[T, R](result: T => R)(implicit arbitrary: Arbitrary[T],
                                 pretty: T => Pretty,
                                 prettyFreqMap: FreqMap[Set[Any]] => Pretty,
                                 asResult: AsResult[R],
                                 parameters: Parameters): ScalaCheckFunction1[T, R] = {
    ScalaCheckFunction1(
      execute = result,
      arbitrary = arbitrary,
      shrink = None,
      collectors = Nil,
      pretty,
      prettyFreqMap,
      asResult,
      context = None,
      parameters
    )
  }

  /** create a ScalaCheck property from a function of 2 arguments */
  def prop[T1, T2, R](result: (T1, T2) => R)(implicit arbitrary1: Arbitrary[T1],
                                             pretty1: T1 => Pretty,
                                             arbitrary2: Arbitrary[T2],
                                             pretty2: T2 => Pretty,
                                             prettyFreqMap: FreqMap[Set[Any]] => Pretty,
                                             asResult: AsResult[R],
                                             parameters: Parameters): ScalaCheckFunction2[T1, T2, R] = {
    ScalaCheckFunction2(
      result,
      ScalaCheckArgInstances(arbitrary1, shrink = None, collectors = Nil, pretty = pretty1),
      ScalaCheckArgInstances(arbitrary2, shrink = None, collectors = Nil, pretty = pretty2),
      prettyFreqMap,
      asResult,
      context = None,
      parameters
    )
  }

  /** create a ScalaCheck property from a function of 3 arguments */
  def prop[T1, T2, T3, R](result: (T1, T2, T3) => R)(implicit arbitrary1: Arbitrary[T1],
                                                     pretty1: T1 => Pretty,
                                                     arbitrary2: Arbitrary[T2],
                                                     pretty2: T2 => Pretty,
                                                     arbitrary3: Arbitrary[T3],
                                                     pretty3: T3 => Pretty,
                                                     prettyFreqMap: FreqMap[Set[Any]] => Pretty,
                                                     asResult: AsResult[R],
                                                     parameters: Parameters): ScalaCheckFunction3[T1, T2, T3, R] = {
    ScalaCheckFunction3(
      result,
      ScalaCheckArgInstances(arbitrary1, shrink = None, collectors = Nil, pretty = pretty1),
      ScalaCheckArgInstances(arbitrary2, shrink = None, collectors = Nil, pretty = pretty2),
      ScalaCheckArgInstances(arbitrary3, shrink = None, collectors = Nil, pretty = pretty3),
      prettyFreqMap = prettyFreqMap,
      asResult,
      context = None,
      parameters
    )
  }
}
