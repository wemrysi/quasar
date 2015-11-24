package quasar
package fp

import scalaz._

object interpret {
  type Coproduct3[F[_], G[_], H[_], A] = Coproduct[F, Coproduct[G, H, ?], A]
  type Coproduct4[F[_], G[_], H[_], I[_], A] = Coproduct[F, Coproduct3[G, H, I, ?], A]
  type Coproduct5[F[_], G[_], H[_], I[_], J[_], A] = Coproduct[F, Coproduct4[G, H, I, J, ?], A]

  def injectedNT[F[_], G[_]](f: F ~> F)(implicit G: F :<: G): G ~> G =
    new (G ~> G) {
      def apply[A](ga: G[A]) = G.prj(ga).fold(ga)(fa => G.inj(f(fa)))
    }

  def interpret2[F[_], G[_], M[_]](f: F ~> M, g: G ~> M): Coproduct[F, G, ?] ~> M =
    new (Coproduct[F, G, ?] ~> M) {
      def apply[A](fa: Coproduct[F, G, A]) =
        fa.run.fold(f, g)
    }

  def interpret3[F[_], G[_], H[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M): Coproduct3[F, G, H, ?] ~> M =
    new (Coproduct3[F, G, H, ?] ~> M) {
      def apply[A](fa: Coproduct3[F, G, H, A]) =
        fa.run.fold(f, interpret2(g, h)(_))
    }

  def interpret4[F[_], G[_], H[_], I[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M, i: I ~> M): Coproduct4[F, G, H, I, ?] ~> M =
    new (Coproduct4[F, G, H, I, ?] ~> M) {
      def apply[A](fa: Coproduct4[F, G, H, I, A]) =
        fa.run.fold(f, interpret3(g, h, i)(_))
    }

  def interpret5[F[_], G[_], H[_], I[_], J[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M, i: I ~> M, j: J ~> M): Coproduct5[F, G, H, I, J, ?] ~> M =
    new (Coproduct5[F, G, H, I, J, ?] ~> M) {
      def apply[A](fa: Coproduct5[F, G, H, I, J, A]) =
        fa.run.fold(f, interpret4(g, h, i, j)(_))
    }

  trait Interpreter[ALGEBRA[_], M[_]] {

    import scala.collection.IndexedSeq
    import scalaz.stream.Process

    type F[A] = Free[ALGEBRA,A]

    val interpretTerm: ALGEBRA ~> M

    def interpret(implicit functor:Functor[ALGEBRA], monad: Monad[M]): F ~> M =
      hoistFree(interpretTerm)

    def interpretT[T[_[_],_]: Hoist](implicit functor:Functor[ALGEBRA], monad: Monad[M]): T[F,?] ~> T[M,?] =
      Hoist[T].hoist[F,M](interpret)

    def interpretT2[T1[_[_],_]: Hoist, T2[_[_],_]: Hoist](implicit functor:Functor[ALGEBRA], monad: Monad[M]): T1[T2[F,?],?] ~> T1[T2[M,?],?] =
      Hoist[T1].hoist[T2[F,?],T2[M,?]](interpretT[T2])(Hoist[T2].apply[F])

    def runLog[A](p: Process[F,A])(implicit functor: Functor[ALGEBRA], monad:Monad[M], catchable: Catchable[M]): M[IndexedSeq[A]] =
      p.translate(interpret).runLog

    // Would be possible if `MonadTrans` had a notion of `Catchable`. Instead we need to copy paste and
    // replace the value of T with a particular Monad Transformer
    // def runLogT[T[_[_],_]:Hoist,A](p: Process[T[F,?],A]): T[TaskMemState,IndexedSeq[A]] = {
    //   p.translate[T[TaskMemState,?]](interpretTaskT[T]).runLog
    // }

    // hard coded `MonadTrans`

    def runLog[E,A](p: Process[EitherT[F,E,?],A])(implicit functor: Functor[ALGEBRA], monad:Monad[M], catchable: Catchable[M]): EitherT[M,E,IndexedSeq[A]] = {
      type T[F[_],A] = EitherT[F,E,A]
      type ResultT[A] = T[M,A]
      p.translate[ResultT](interpretT[T]).runLog[ResultT,A]
    }
    def runLog[E,L:Monoid,A](p: Process[EitherT[WriterT[F,L,?],E,?],A])(implicit functor: Functor[ALGEBRA], monad:Monad[M], catchable: Catchable[M]): EitherT[WriterT[M,L,?],E,IndexedSeq[A]] = {
      type OutsideT[F[_],A] = EitherT[F,E,A]
      type T[F[_],A] = OutsideT[WriterT[F,L,?],A]
      type ResultT[A] = T[M,A]
      // inference is hard... let's help Scalac out a bit...
      val monadR: Monad[ResultT] = EitherT.eitherTMonad[WriterT[M,L,?],E](WriterT.writerTMonad[M,L])
      val catchableR: Catchable[ResultT] = eitherTCatchable[WriterT[M,L,?],E](writerTCatchable[M,L], WriterT.writerTFunctor[M,L])
      p.translate[ResultT](interpretT2[EitherT[?[_],E,?], WriterT[?[_],L,?]](EitherT.eitherTHoist,WriterT.writerTHoist[L], functor, monad)).runLog[ResultT,A](monadR,catchableR)
    }
  }
}
