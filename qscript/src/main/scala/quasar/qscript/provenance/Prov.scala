/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.qscript.provenance

import slamdata.Predef._

import quasar.contrib.matryoshka.birecursiveIso
import quasar.contrib.scalaz.MonadTell_
import quasar.fp.ski.{ι, κ}

import matryoshka._
import matryoshka.implicits._

import monocle.Prism

import scalaz._, Scalaz._
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

/**
  * @tparam D type of scalar identity
  * @tparam I type of vector identity
  * @tparam S sorts of identities
  * @tparam P provenance representation
  *
  */
trait Prov[D, I, S, P] {
  type PF[A] = ProvF[D, I, S, A]

  implicit def PC: Corecursive.Aux[P, PF]
  implicit def PR: Recursive.Aux[P, PF]

  def staticId: Prism[I, D]

  import ProvF._
  private val O = ProvF.Optics[D, I, S]

  // Optics

  def fresh: Prism[P, Unit] =
    birecursiveIso[P, PF] composePrism O.fresh[P]

  def project: Prism[P, (D, S)] =
    birecursiveIso[P, PF] composePrism O.project[P]

  def inject: Prism[P, (D, S)] =
    birecursiveIso[P, PF] composePrism O.inject[P]

  def inflate: Prism[P, (I, S)] =
    birecursiveIso[P, PF] composePrism O.inflate[P]

  def both: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.both[P]

  def thenn: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.thenn[P]

  object implicits {
    implicit final class ProvOps(p: P) {
      def ∧(q: P)(implicit D: Equal[D], I: Equal[I], S: Equal[S]): P =
        provConjunctionSemiLattice.append(Conjunction(p), Conjunction(q)).unwrap

      def ≺:(q: P): P =
        thenn(q, p)

      def conjunction: P @@ Conjunction = Conjunction(p)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Recursion"))
    implicit def provEqual(implicit D: Equal[D], I: Equal[I], S: Equal[S]): Equal[P] =
      Equal.equal((x, y) => (x.project, y.project) match {
        case (a@Fresh(), b@Fresh()) => a eq b
        case (Inflate(l, sl), Inflate(r, sr)) => l ≟ r && sl ≟ sr
        case (Project(l, sl), Project(r, sr)) => l ≟ r && sl ≟ sr
        case (Inject(l, sl), Inject(r, sr)) => l ≟ r && sl ≟ sr
        case (Both(_, _), _) => AsSet(flattenBoth(x)) ≟ AsSet(flattenBoth(y))
        case (_, Both(_, _)) => AsSet(flattenBoth(x)) ≟ AsSet(flattenBoth(y))
        case (Then(_, _), _) => flattenThen(x) ≟ flattenThen(y)
        case (_, Then(_, _)) => flattenThen(x) ≟ flattenThen(y)
        case _ => false
      })

    implicit def provConjunctionSemiLattice(implicit D: Equal[D], I: Equal[I], S: Equal[S])
        : SemiLattice[P @@ Conjunction] =
      new SemiLattice[P @@ Conjunction] {
        def append(a: P @@ Conjunction, b: => P @@ Conjunction) =
          Conjunction(zipWhileEq(a.unwrap, b.unwrap))

        /** Takes the longest sequence prefix common to both sides, conjoining
          * whatever is leftover as the head.
          *
          * Example: (z ≺ y ≺ x) ∧ (w ≺ y ≺ x) == (z ∧ w) ≺ y ≺ x
          */
        def zipWhileEq(l: P, r: P): P = {
          @tailrec
          def zipWhileEq0(ls: IList[P], rs: IList[P], out: P): P =
            (ls, rs) match {
              case (ICons(lh, lt), ICons(rh, rt)) if lh === rh =>
                zipWhileEq0(lt, rt, lh ≺: out)

              case (ICons(lh, lt), ICons(rh, rt)) =>
                val lout = lt.foldLeft(lh)((o, p) => p ≺: o)
                val rout = rt.foldLeft(rh)((o, p) => p ≺: o)
                distinctConjunctions(both(lout, rout)) ≺: out

              case (ICons(_, _), INil()) =>
                ls.foldLeft(out)((o, p) => p ≺: o)

              case (INil(), ICons(_, _)) =>
                rs.foldLeft(out)((o, p) => p ≺: o)

              case (INil(), INil()) =>
                out
            }

          val ls = flattenThen(l).reverse
          val rs = flattenThen(r).reverse

          if (ls.head === rs.head)
            zipWhileEq0(ls.tail, rs.tail, ls.head)
          else
            distinctConjunctions(both(l, r))
      }
    }

    implicit def provSequenceSemigroup: Semigroup[P] =
      Semigroup.instance(thenn(_, _))
  }

  import implicits._

  /** Returns whether the provenance autojoin, collecting join keys using the
    * specified monad.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def autojoined[F[_]: Monad](left: P, right: P)(
      implicit
      F: MonadTell_[F, JoinKeys[I]],
      D: Equal[D],
      S: Equal[S])
      : F[Boolean] = {

    // NB: We don't want to consider identities themselves when checking for
    //     joinability, just that both sides are comparable.
    implicit val ignoreI: Equal[I] = Equal.equalBy(κ(()))

    def joinBoths(l0: P, r0: P): F[Boolean] = {
      val crossed =
        flattenBoth(l0) tuple flattenBoth(r0)

      val joined =
        Disjunction.unsubst(crossed foldMapM {
          case (l, r) => Disjunction.subst(autojoined[Writer[JoinKeys[I], ?]](l, r))
        })

      joined.run match {
        case (jks, b) =>
          val intersected = jks.keys.foldMap1Opt(Foldable1[NonEmptyList].fold1(_)) match {
            case Some(isect) => JoinKeys(IList(NonEmptyList(isect)))
            case None => JoinKeys.empty[I]
          }

          F.writer(intersected, b)
      }
    }

    def joinThens(l0: P, r0: P): F[Boolean] = {
      val ls = flattenThen(l0).reverse
      val rs = flattenThen(r0).reverse

      val joined =
        ls.alignBoth(rs).foldLeftM(true) {
          case (true, Some((l, r))) => autojoined[Writer[JoinKeys[I], ?]](l, r)
          case _ => false.point[Writer[JoinKeys[I], ?]]
        }

      joined.run match {
        case (jks, b) => F.writer(JoinKeys(jks.keys.foldMap1Opt(ι).toIList), b)
      }
    }

    (left.project, right.project) match {
      case (Inflate(l, sl), Inflate(r, sr)) if sl ≟ sr => F.writer(JoinKeys.singleton(l, r), true)
      case (Project(l, sl), Inflate(r, sr)) if sl ≟ sr => F.writer(JoinKeys.singleton(staticId(l), r), true)
      case (Inflate(l, sl), Project(r, sr)) if sl ≟ sr => F.writer(JoinKeys.singleton(l, staticId(r)), true)
      case (Both(_, _), _) => joinBoths(left, right)
      case (_, Both(_, _)) => joinBoths(left, right)
      case (Then(_, _), _) => joinThens(left, right)
      case (_, Then(_, _)) => joinThens(left, right)
      case _ => (left ≟ right).point[F]
    }
  }

  /** Remove duplicate values from conjunctions. */
  def distinctConjunctions(p: P)(implicit D: Equal[D], I: Equal[I], S: Equal[S]): P = {
    def distinctConjunction(conj: NonEmptyList[P]): P =
      conj.distinctE1.foldRight1(both(_, _))

    val distinctConjunctionsƒ: ElgotAlgebra[(P, ?), PF, NonEmptyList[P]] = {
      case (_, Both(l, r)) => l append r
      case (_, Then(f, s)) => NonEmptyList(thenn(distinctConjunction(f), distinctConjunction(s)))
      case (other, _) => NonEmptyList(other)
    }

    distinctConjunction(p.elgotPara(distinctConjunctionsƒ))
  }

  /** Attempt to reduce a sequence ending in a projection.
    *
    * A success indicates there was no projection or it was applied successfully,
    * possibly eliminating provenance entirely.
    *
    * A failure indicates the projection is statically known to result in undefined.
    */
  def applyProjection(p: P)(implicit eqD: Equal[D], eqI: Equal[I], eqS: Equal[S])
      : Validation[Unit, Option[P]] = {

    def applyProjection0(key: D, sort: S, from: P): Option[(Boolean, IList[P])] = {
      val (foundKey, join) = flattenBoth(from) foldMap {
        case Embed(Inject(k, s)) =>
          ((k ≟ key && s ≟ sort).disjunction, IList[NonEmptyList[P] \/ (Boolean, P)]())

        case Embed(Then(Embed(Inject(k, s)), snd)) =>
          ((k ≟ key && s ≟ sort).disjunction, IList(((k ≟ key && s ≟ sort), snd).right[NonEmptyList[P]]))

        case other =>
          (false.disjunction, IList(flattenThen(other).left[(Boolean, P)]))
      }

      val joined =
        if (foundKey.unwrap)
          some(join.flatMap(
            _.fold(_.tail, r => if (r._1) IList(r._2) else IList[P]())
              .toNel
              .map(_.foldMap1(ι))
              .toIList))
        else if (join.all(_.isRight))
          none[IList[P]]
        else
          some(join.map(_.fold(_.foldMap1(ι), _._2)))

      joined strengthL foundKey.unwrap
    }

    flattenThen(p) match {
      case NonEmptyList(Embed(Project(k, s)), ICons(r, rs)) =>
        applyProjection0(k, s, r).toSuccess(()) map { ps0 =>
          val ps = ps0.map(_.toNel) match {
            case (true, Some(jn)) =>
              some(NonEmptyList.nel(jn.foldMap1(_.conjunction).unwrap, rs))

            case (false, Some(jn)) =>
              some(NonEmptyList.nel(project(k, s), jn.foldMap1(_.conjunction).unwrap :: rs))

            case (_, None) =>
              rs.toNel
          }

          ps.map(_.foldMap1(ι))
        }

      case _ => Success(some(p))
    }
  }

  /** Flatten a conjunction into a `NonEmptyList`. */
  def flattenBoth(p: P): NonEmptyList[P] =
    p.elgotPara[NonEmptyList[P]] {
      case (_, Both(l, r)) => l append r
      case (other, _) => NonEmptyList(other)
    }

  /** Flatten a sequence into a `NonEmptyList`. */
  def flattenThen(p: P): NonEmptyList[P] =
    p.elgotPara[NonEmptyList[P]] {
      case (_, Then(h, t)) => h append t
      case (other, _) => NonEmptyList(other)
    }
}

object Prov {
  def apply[D, I, S, P](staticP: Prism[I, D])(
      implicit
      TC: Corecursive.Aux[P, ProvF[D, I, S, ?]],
      TR: Recursive.Aux[P, ProvF[D, I, S, ?]])
      : Prov[D, I, S, P] =
    new Prov[D, I, S, P] {
      val PC = TC
      val PR = TR
      val staticId = staticP
    }
}
