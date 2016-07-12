package blueeyes

import scalaz._

class FutureMonad(context: ExecutionContext) extends Applicative[Future] with Monad[Future] {
  private implicit def ec: ExecutionContext = context
  def point[A](a: => A): Future[A]                                          = Future(a)
  def bind[A, B](fut: Future[A])(f: A => Future[B]): Future[B]              = fut.flatMap(f)
  override def ap[A, B](fa: => Future[A])(ff: => Future[A => B]): Future[B] = (fa zip ff) map { case (a, f) => f(a) }
}

class UnsafeFutureComonad(context: ExecutionContext, copointMaxWait: Duration) extends FutureMonad(context) with Comonad[Future] {
  private implicit def ec: ExecutionContext = context
  def copoint[A](m: Future[A])                                  = Await.result(m, copointMaxWait)
  def cojoin[A](a: Future[A]): Future[Future[A]]                = Promise.successful(a).future
  def cobind[A, B](fa: Future[A])(f: Future[A] => B): Future[B] = Promise.successful(f(fa)).future
}

object FutureMonad {
  def M(implicit context: ExecutionContext): Monad[Future] = new FutureMonad(context)
}
