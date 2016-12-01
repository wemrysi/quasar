package blueeyes
package core.data

trait AsyncTranscoder[F[_], G[_], A, B] {
  def apply(a: F[A]): F[B]
  def unapply(fb: Future[G[B]]): Future[G[A]]
}
