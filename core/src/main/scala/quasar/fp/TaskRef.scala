package quasar
package fp

import quasar.Predef.Unit

import java.util.concurrent.atomic.AtomicReference
import scalaz.concurrent.Task

/** A thread-safe, atomically updateable mutable reference. */
sealed abstract class TaskRef[A] {
  def read: Task[A]
  def write(a: A): Task[Unit]
  def modify(f: A => A): Task[A]
}

object TaskRef {
  def apply[A](initial: A): Task[TaskRef[A]] = Task delay {
    new TaskRef[A] {
      val ref = new AtomicReference(initial)
      def read = Task.delay(ref.get)
      def write(a: A) = Task.delay(ref.set(a))
      def modify(f: A => A) = for {
        a0 <- read
        a1 =  f(a0)
        p  <- Task.delay(ref.compareAndSet(a0, a1))
        a  <- if (p) read else modify(f)
      } yield a
    }
  }
}
