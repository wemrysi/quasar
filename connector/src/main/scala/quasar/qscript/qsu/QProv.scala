/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.qscript.qsu

import slamdata.Predef.{Eq => _, Int => SInt, _}
import quasar.contrib.scalaz.MonadState_
import quasar.ejson._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.qscript.provenance._

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.data._
import scalaz.{Lens => _, _}, Scalaz._, Tags.LastVal

import MapFuncsCore._

final class QProv[T[_[_]]: BirecursiveT: EqualT]
    extends Dimension[T[EJson], FreeMapA[T, Access[Symbol]], QProv.P[T]]
    with QSUTTypes[T] {

  type D     = T[EJson]
  type I     = FreeAccess[Symbol]
  type PF[A] = QProv.PF[T, A]
  type P     = QProv.P[T]

  val prov: Prov[D, I, P] =
    Prov[D, I, P](ejs => Free.roll(MFC[T, I](Constant(ejs))))

  /** The `JoinFunc` representing the autojoin of the given dimensions. */
  def autojoinCondition(ls: Dimensions[P], rs: Dimensions[P]): FreeAccess[JoinSide] = {
    def eqCond(k: JoinKeys.JoinKey[I]): FreeAccess[JoinSide] =
      Free.roll(MFC(Eq(
        FMA.map(k.left)(κ(LeftSide)),
        FMA.map(k.right)(κ(RightSide)))))

    autojoinKeys(ls, rs).keys.toNel.fold[FreeAccess[JoinSide]](BoolLit(true)) { jks =>
      jks.foldMapLeft1(eqCond)((l, r) => Free.roll(MFC(And(l, eqCond(r)))))
    }
  }

  /** Returns a position map of the identities present in the given provenance
    * and a new provenance where the identities have been replaced with the
    * appropriate bucket references.
    */
  def bucketedIds[M[_]: Monad: MonadState_[?[_], SInt]](src: Symbol, p: P): M[(P, SInt ==>> I)] =
    p.cataM(bucketedIdsƒ[M](src))

  def bucketedIdsƒ[M[_]: Monad](src: Symbol)(implicit M: MonadState_[M, SInt]): AlgebraM[M, PF, (P, SInt ==>> I)] = {
    def children
        (l: (P, SInt ==>> I), r: (P, SInt ==>> I))
        (f: (P, P) => P)
        : M[(P, SInt ==>> I)] =
      (l, r) match {
        case ((lp, lg), (rp, rg)) => (f(lp, rp), lg union rg).point[M]
      }

    {
      case ProvF.Value(i) =>
        for {
          idx <- M.get
          _   <- M.put(idx + 1)
          b   =  Free.point(Access.bucket(src, idx, src)) : I
        } yield (prov.value(b), IMap.singleton(idx, i))

      case ProvF.Nada()      => (prov.nada(), IMap.empty[SInt, I]).point[M]
      case ProvF.Proj(ejs)   => (prov.proj(ejs), IMap.empty[SInt, I]).point[M]
      case ProvF.Both(l, r)  => children(l, r)(prov.both(_, _))
      case ProvF.OneOf(l, r) => children(l, r)(prov.oneOf(_, _))
      case ProvF.Then(l, r)  => children(l, r)(prov.thenn(_, _))
    }
  }

  /** Converts identity access in `dims` to bucket access of `src`. */
  def bucketAccess(src: Symbol, dims: Dimensions[P]): Dimensions[P] =
    bucketedDims(src, dims)._1

  /** Reifies the identities in the given `Dimensions`. */
  def buckets(dims: Dimensions[P]): List[I] = {
    val maps = bucketedDims(Symbol(""), dims)._2
    LastVal.unsubst(maps.foldMap(LastVal.subst(_))).values
  }

  /** Renames `from` to `to` in the given dimensions. */
  def rename(from: Symbol, to: Symbol, dims: Dimensions[P]): Dimensions[P] = {
    def rename0(sym: Symbol): Symbol =
      (sym === from) ? to | sym

    val renameAccess =
      Access.symbols.modify(rename0) <<< Access.src.modify(rename0)

    canonicalize(dims map (_.transCata[P](pfo.value modify (_ map renameAccess))))
  }

  ////

  private val pfo = ProvF.Optics[D, I]
  private val FMA = Functor[FreeMapA].compose[Access]

  // NB: Computed together to ensure indices align properly.
  private def bucketedDims(src: Symbol, dims: Dimensions[P]): (Dimensions[P], IList[SInt ==>> I]) =
    dims.reverse
      .traverse(bucketedIds[State[SInt, ?]](src, _))
      .eval(0)
      .unzip
      .leftMap(_.reverse)
}

object QProv {
  type PF[T[_[_]], A] = ProvF[T[EJson], FreeMapA[T, Access[Symbol]], A]
  type P[T[_[_]]]     = T[PF[T, ?]]

  def apply[T[_[_]]: BirecursiveT: EqualT]: QProv[T] =
    new QProv[T]
}
