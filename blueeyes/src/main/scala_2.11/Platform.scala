// scala 2.11
package blueeyesstubs {
  import scalaz._, Scalaz._

  trait Platform {
    type ExecutionContext = java.util.concurrent.ExecutorService
    type Duration         = scala.concurrent.duration.Duration
    type Future[+A]       = scalaz.concurrent.Future[A]
    // type Promise[A]       = scalaz.concurrent.Promise[A]
    type Task[+A]         = scalaz.concurrent.Task[A]

    // val ExecutionContext = scala.concurrent.ExecutionContext
    // val Await            = scala.concurrent.Await
    val Future           = scalaz.concurrent.Future
    // val Promise          = scalaz.concurrent.Promise
    val Duration         = scala.concurrent.duration.Duration
    // val Task             = scalaz.concurrent.Task

    type BigDecimal = scala.math.BigDecimal
    val BigDecimal  = scala.math.BigDecimal

    implicit def bigDecimalOrder: scalaz.Order[blueeyes.BigDecimal] =
      scalaz.Order.order((x, y) => Ordering.fromInt(x compare y))

    def futureMonad(ec: ExecutionContext): Monad[Future] = implicitly

    implicit class ScalaSeqOps[A](xs: scala.collection.Seq[A]) {
      def sortMe(implicit z: scalaz.Order[A]): Vector[A] =
        xs sortWith ((a, b) => z.order(a, b) == Ordering.LT) toVector
    }
  }
}
