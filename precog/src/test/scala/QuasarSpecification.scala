package quasar

import precog._, TestSupport._
import org.specs2.mutable._
import org.specs2.execute.AsResult
import scalaz._

trait QuasarSpecification extends SpecificationLike with ScalaCheck {
  // Report all test timings.
  args.report(showtimes=true)

  /** Allows marking non-deterministically failing tests as such,
   *  in the manner of pendingUntilFixed but such that it will not
   *  fail regardless of whether it seems to pass or fail.
   */
  implicit class FlakyTest[T : AsResult](t: => T) {
    import org.specs2.execute._
    def flakyTest: Result = flakyTest("")
    def flakyTest(m: String): Result = ResultExecution.execute(AsResult(t)) match {
      case s: Success => s
      case r          =>
        val explain = if (m == "") "" else s" ($m)"
        Skipped(s"${r.message}, but test is marked as flaky$explain", r.expected)
    }
  }
}
