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
import scalaz.Tags.Disjunction
import scalaz.{NonEmptyList => NEL, _}, Scalaz._

/**
  * @tparam D type of data
  * @tparam I type of identity
  * @tparam P provenance representation
  *
  */
trait Prov[D, I, P] {
  type PF[A] = ProvF[D, I, A]

  implicit def PC: Corecursive.Aux[P, PF]
  implicit def PR: Recursive.Aux[P, PF]

  import ProvF._
  private val O = ProvF.Optics[D, I]

  // Optics

  def nada: Prism[P, Unit] =
    birecursiveIso[P, PF] composePrism O.nada[P]

  def fresh: Prism[P, Unit] =
    birecursiveIso[P, PF] composePrism O.fresh[P]

  def prjPath: Prism[P, D] =
    birecursiveIso[P, PF] composePrism O.prjPath[P]

  def prjValue: Prism[P, D] =
    birecursiveIso[P, PF] composePrism O.prjValue[P]

  def injValue: Prism[P, D] =
    birecursiveIso[P, PF] composePrism O.injValue[P]

  def value: Prism[P, I] =
    birecursiveIso[P, PF] composePrism O.value[P]

  def both: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.both[P]

  def oneOf: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.oneOf[P]

  def thenn: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.thenn[P]

  // Operations

  /** Returns whether the provenance autojoin, collecting join keys using the
    * specified monad.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def autojoined[F[_]: Monad](left: P, right: P)(
      implicit
      F: MonadTell_[F, JoinKeys[I]],
      D: Equal[D])
      : F[Boolean] = {

    // NB: We don't want to consider identities themselves when checking for
    //     joinability, just that both sides are Value(_).
    implicit val ignoreI: Equal[I] = Equal.equalBy(κ(()))

    def joinBoths(l0: P, r0: P): F[Boolean] = {
      val crossed =
        flattenBoth(l0).join tuple flattenBoth(r0).join

      val joined =
        Disjunction.unsubst(crossed foldMapM {
          case (l, r) => Disjunction.subst(autojoined[Writer[JoinKeys[I], ?]](l, r))
        })

      joined.run match {
        case (jks, b) => F.writer(JoinKeys(jks.keys.foldMap1Opt(ι).toIList), b)
      }
    }

    def joinOneOfs(l0: P, r0: P): F[Boolean] = {
      val crossed =
        flattenOneOf(l0) tuple flattenOneOf(r0)

      val joined =
        Disjunction.unsubst(crossed foldMapM {
          case (l, r) => Disjunction.subst(autojoined[Writer[JoinKeys[I], ?]](l, r))
        })

      joined.run match {
        case (jks, b) => F.writer(JoinKeys(jks.keys.foldMap1Opt(ι).toIList), b)
      }
    }

    def joinThens(l0: P, r0: P): F[Boolean] = {
      val crossed =
        flattenThen(l0) tuple flattenThen(r0)

      Disjunction.unsubst(crossed foldMapM {
        case (ls, rs) =>
          Disjunction.subst(ls.reverse.alignBoth(rs.reverse).foldLeftM(true) {
            case (true, Some((l, r))) => autojoined[F](l, r)
            case _ => false.point[F]
          })
      })
    }

    (left.project, right.project) match {
      case (Value(l), Value(r)) => F.writer(JoinKeys.singleton(l, r), true)
      case (Both(_, _), _) => joinBoths(left, right)
      case (_, Both(_, _)) => joinBoths(left, right)
      case (Then(_, _), _) => joinThens(left, right)
      case (_, Then(_, _)) => joinThens(left, right)
      case (OneOf(_, _), _) => joinOneOfs(left, right)
      case (_, OneOf(_, _)) => joinOneOfs(left, right)
      case _ => (left ≟ right).point[F]
    }
  }

  /** Reduce to normal form. */
  def normalize(p: P)(implicit eqD: Equal[D], eqI: Equal[I]): P =
    applyProjection(removeDuplicates(p)).cata(collapseNadaƒ)


  // Instances

  @SuppressWarnings(Array("org.wartremover.warts.Recursion", "org.wartremover.warts.Equals"))
  implicit def provenanceEqual(implicit eqD: Equal[D], eqI: Equal[I]): Equal[P] = {
    implicit def listSetPEqual: Equal[IList[P] @@ AsSet] =
      asSetEqual[IList, P](Foldable[IList], provenanceEqual(eqD, eqI))

    def thenEq(l: P, r: P): Boolean =
      AsSet(flattenThen(l)) ≟ AsSet(flattenThen(r))

    def bothEq(l: P, r: P): Boolean =
      AsSet(flattenBoth(l) map (ls => AsSet(nubNada(ls)))) ≟
        AsSet(flattenBoth(r) map (rs => AsSet(nubNada(rs))))

    def oneOfEq(l: P, r: P): Boolean =
      AsSet(nubNada(flattenOneOf(l))) ≟ AsSet(nubNada(flattenOneOf(r)))

    Equal.equal((x, y) => (x.project, y.project) match {
      case (Nada(), Nada()) => true
      case (a@Fresh(), b@Fresh()) => a eq b
      case (Value(l), Value(r)) => l ≟ r
      case (PrjPath(l), PrjPath(r)) => l ≟ r
      case (PrjValue(l), PrjValue(r)) => l ≟ r
      case (InjValue(l), InjValue(r)) => l ≟ r
      case (Both(_, _), _) => bothEq(x, y)
      case (_, Both(_, _)) => bothEq(x, y)
      case (Then(_, _), _) => thenEq(x, y)
      case (_, Then(_, _)) => thenEq(x, y)
      case (OneOf(_, _), _) => oneOfEq(x, y)
      case (_, OneOf(_, _)) => oneOfEq(x, y)
      case _ => false
    })
  }

  ////

  private def removeDuplicates(p: P)(implicit eqD: Equal[D], eqI: Equal[I]): P = {
    def removeDuplicates0(alternates: NEL[NEL[P]]): P =
      alternates
        .map(_.distinctE1.foldRight1(both(_, _)))
        .distinctE1
        .foldRight1(oneOf(_, _))

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val removeDuplicatesƒ: Algebra[PF, NEL[NEL[P]]] = {
      case Both(l, r) => (l |@| r)(_ append _)
      case OneOf(l, r) => l append r
      case Then(l, r) => NEL(NEL(thenn(removeDuplicates0(l), removeDuplicates0(r))))
      case Value(i) => NEL(NEL(value(i)))
      case InjValue(d) => NEL(NEL(injValue(d)))
      case PrjValue(d) => NEL(NEL(prjValue(d)))
      case PrjPath(d) => NEL(NEL(prjPath(d)))
      case p@Fresh() => NEL(NEL(p.asInstanceOf[PF[P]].embed))
      case Nada() => NEL(NEL(nada()))
    }

    removeDuplicates0(p.cata(removeDuplicatesƒ))
  }

  private def applyProjection(p: P)(implicit eqD: Equal[D], eqI: Equal[I]): P = {
    def applyProjection0(thens: NEL[P]): P =
      thens match {
        case NEL(Embed(PrjValue(k)), ICons(Embed(InjValue(j)), ts)) =>
          (k === j).option(ts)
            .flatMap(_.foldRight1Opt(thenn(_, _)))
            .getOrElse(nada())

        case _ => thens.foldRight1(thenn(_, _))
      }

    // OneOf[Both[Then]]
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val flattenƒ: Algebra[PF, NEL[NEL[NEL[P]]]] = {
      case Then(l, r) => (l |@| r)((l1, r1) => (l1 |@| r1)(_ append _))
      case Both(l, r) => (l |@| r)(_ append _)
      case OneOf(l, r) => l append r
      case Value(i) => NEL(NEL(NEL(value(i))))
      case InjValue(d) => NEL(NEL(NEL(injValue(d))))
      case PrjValue(d) => NEL(NEL(NEL(prjValue(d))))
      case PrjPath(d) => NEL(NEL(NEL(prjPath(d))))
      case p@Fresh() => NEL(NEL(NEL(p.asInstanceOf[PF[P]].embed)))
      case Nada() => NEL(NEL(NEL(nada())))
    }

    p.cata(flattenƒ)
      .map(_.map(applyProjection0).foldRight1(both(_, _)))
      .foldRight1(oneOf(_, _))
  }

  private val collapseNadaƒ: Algebra[PF, P] = {
    case Both(l, Embed(Nada())) => l
    case Both(Embed(Nada()), r) => r
    case Then(Embed(Nada()), _) => nada()
    case Then(_, Embed(Nada())) => nada()
    case pf => pf.embed
  }

  /** The disjunction of sets arising from distributing `OneOf` over trees
    * of `Both`.
    */
  private def flattenBoth(p: P): IList[IList[P]] =
    p.elgotPara[IList[IList[P]]](flattenBothƒ)

  private val flattenBothƒ: ElgotAlgebra[(P, ?), PF, IList[IList[P]]] = {
    case (_, Both(l, r)) => (l |@| r)(_ ++ _)
    case (_, OneOf(l, r)) => l ++ r
    case (other, _) => IList(IList(other))
  }

  /** The disjunction described by a tree of `OneOf`. */
  private def flattenOneOf(p: P): IList[P] =
    p.elgotPara[IList[P]](flattenOneOfƒ)

  private val flattenOneOfƒ: ElgotAlgebra[(P, ?), PF, IList[P]] = {
    case (_, OneOf(l, r)) => l ++ r
    case (other, _) => IList(other)
  }

  /** The disjunction of sequences arising from distributing `OneOf` over trees
    * of `Then`.
    */
  private def flattenThen(p: P): IList[IList[P]] =
    p.elgotPara[IList[IList[P]]](flattenThenƒ)

  private val flattenThenƒ: ElgotAlgebra[(P, ?), PF, IList[IList[P]]] = {
    case (_, Then(l, r)) => (l |@| r)(_ ++ _)
    case (_, OneOf(l, r)) => l ++ r
    case (other, _) => IList(IList(other))
  }

  private def nubNada(ps: IList[P]): IList[P] =
    ps.filter(nada.isEmpty)
}

object Prov {
  def apply[D, I, P](
      implicit
      TC: Corecursive.Aux[P, ProvF[D, I, ?]],
      TR: Recursive.Aux[P, ProvF[D, I, ?]])
      : Prov[D, I, P] =
    new Prov[D, I, P] {
      val PC = TC
      val PR = TR
    }
}
