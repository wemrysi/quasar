package object blueeyes {
  type ActorRef         = akka.actor.ActorRef
  type ActorSystem      = akka.actor.ActorSystem
  type Duration         = akka.util.Duration
  type DurationLong     = akka.util.DurationLong
  type ExecutionContext = akka.dispatch.ExecutionContext
  type Future[+A]       = akka.dispatch.Future[A]
  type Promise[A]       = akka.dispatch.Promise[A]
  type Timeout          = akka.util.Timeout

  val ActorSystem      = akka.actor.ActorSystem
  val AkkaProps        = akka.actor.Props
  val Await            = akka.dispatch.Await
  val ExecutionContext = akka.dispatch.ExecutionContext
  val Future           = akka.dispatch.Future
  val Promise          = akka.dispatch.Promise
  val Timeout          = akka.util.Timeout

  type spec           = scala.specialized
  type tailrec        = scala.annotation.tailrec
  type switch         = scala.annotation.switch
  type ArrayBuffer[A] = scala.collection.mutable.ArrayBuffer[A]
  type ListBuffer[A]  = scala.collection.mutable.ListBuffer[A]
  val ArrayBuffer     = scala.collection.mutable.ArrayBuffer

  implicit def K[A](a: A): util.K[A]                          = new util.K(a)
  implicit def OptStr(s: Option[String]): util.OptStr         = new util.OptStr(s)
  implicit def IOClose[T <: java.io.Closeable]: util.Close[T] = new util.Close[T] { def close(a: T) = a.close }

  def lp[T](label: String): T => Unit               = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit = (t: T) => println(label + ": " + f(t))

}

package blueeyes {
  package util {
    trait Close[A] {
      def close(a: A): Unit
    }

    case class K[A](a: A) {
      def ->-(f: A => Any): A = { f(a); a }
    }

    case class OptStr(s: Option[String]) {
      def str(f: String => String) = s.map(f).getOrElse("")
    }
  }
}
