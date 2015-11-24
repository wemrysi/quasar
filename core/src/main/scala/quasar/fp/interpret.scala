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

  class Interpreter[F[_]: Functor, M[_]: Monad](val interpretTerm: F ~> M) {

    import scala.collection.IndexedSeq
    import scalaz.stream.Process

    type Program[A] = Free[F,A]

    def interpret: Program ~> M =
      hoistFree(interpretTerm)

    def interpretT[T[_[_],_]: Hoist]: T[Program,?] ~> T[M,?] =
      Hoist[T].hoist[Program,M](interpret)

    def interpretT2[T1[_[_],_]: Hoist, T2[_[_],_]: Hoist]: T1[T2[Program,?],?] ~> T1[T2[M,?],?] =
      Hoist[T1].hoist[T2[Program,?],T2[M,?]](interpretT[T2])(Hoist[T2].apply[Program])

    def runLog[A](p: Process[Program,A])(implicit catchable: Catchable[M]): M[IndexedSeq[A]] =
      p.translate(interpret).runLog

    def runLogT[T[_[_],_]:Hoist,A](p: Process[T[Program,?],A])(implicit catchable: Catchable[T[M,?]]): T[M,IndexedSeq[A]] = {
      type ResultT[A] = T[M,A]
      val monadR: Monad[ResultT] = Hoist[T].apply
      p.translate[T[M,?]](interpretT[T]).runLog[ResultT,A](monadR, catchable)
    }

    def runLogT2[T1[_[_],_]: Hoist, T2[_[_],_]: Hoist,A](p: Process[T1[T2[Program,?],?],A])(implicit catchable: Catchable[T1[T2[M,?],?]]): T1[T2[M,?],IndexedSeq[A]] = {
      type ResultT[A] = T1[T2[M,?],A]
      val monadR: Monad[ResultT] = Hoist[T1].apply[T2[M,?]](Hoist[T2].apply)
      p.translate[ResultT](interpretT2[T1,T2]).runLog[ResultT,A](monadR,catchable)
    }

    // Specialized for type inference

    def runLog[E,A](p: Process[EitherT[Program,E,?],A])(implicit catchable: Catchable[M]): EitherT[M,E,IndexedSeq[A]] = {
      type T[Program[_],A] = EitherT[Program,E,A]
      runLogT[T,A](p)
    }
    def runLog[E,L:Monoid,A](p: Process[EitherT[WriterT[Program,L,?],E,?],A])(implicit catchable: Catchable[M]): EitherT[WriterT[M,L,?],E,IndexedSeq[A]] = {
      type T1[M[_],A] = EitherT[M,E,A]
      type T2[M[_],A] = WriterT[M,L,A]
      type WriterResult[A] = T2[M,A]
      type ResultT[A] = T1[T2[M,?],A]
      val catchableR: Catchable[ResultT] = eitherTCatchable[WriterResult,E](writerTCatchable[M,L], WriterT.writerTFunctor[M,L])
      runLogT2[T1,T2,A](p)(Hoist[T1],Hoist[T2],catchableR)
    }
  }
}
