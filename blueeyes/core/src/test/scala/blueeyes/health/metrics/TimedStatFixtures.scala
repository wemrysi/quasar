package blueeyes
package health.metrics

trait TimedStatFixtures {
  protected implicit val clock = new TestClock

  class TestClock extends blueeyes.util.Clock {
    private var _now: Long = 0

    def now()      = new DateTime(_now)
    def instant()  = now().toInstant
    def nanoTime() = sys.error("Not required for test.")

    def setNow(value: Long) {
      _now = value
    }
  }
}
