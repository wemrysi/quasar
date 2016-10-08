/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import scala.Predef.$conforms
import quasar.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp._
import quasar.fp.ski._

import matryoshka._
import scalaz._, Scalaz._

/** Centralizes the knowledge of T[_[_]] as well as certain
 *  type classes required for many operations. This is for
 *  compilation performance.
 */

trait TTypes[T[_[_]]] {
  // Partially applying types with the known T.
  // In this context we shouldn't often need to refer to the original type
  // any longer, so reuse the name.
  type EquiJoin[A]      = quasar.qscript.EquiJoin[T, A]
  type QScriptCore[A]   = quasar.qscript.QScriptCore[T, A]
  type QScriptTotal[A]  = quasar.qscript.QScriptTotal[T, A]
  type ProjectBucket[A] = quasar.qscript.ProjectBucket[T, A]
  type ThetaJoin[A]     = quasar.qscript.ThetaJoin[T, A]
  type MapFunc[A]       = quasar.qscript.MapFunc[T, A]
  type FreeQS           = quasar.qscript.FreeQS[T]
}

object TTypes {
  def normalizable[T[_[_]] : Recursive : Corecursive : EqualT : ShowT] = new NormalizableT[T]
  def simplifiableProjection[T[_[_]]]                                  = new SimplifiableProjectionT[T]
}

class SimplifiableProjectionT[T[_[_]]] extends TTypes[T] {
  import SimplifyProjection._

  private lazy val simplify: EndoK[QScriptTotal] = simplifyQScriptTotal[T].simplifyProjection
  private def applyToBranch(branch: FreeQS): FreeQS = branch mapSuspension simplify

  def ProjectBucket[G[_]](implicit QC: QScriptCore :<: G) = make(
    λ[ProjectBucket ~> G] {
      case BucketField(src, value, field) => QC inj Map(src, Free roll MapFuncs.ProjectField(value, field))
      case BucketIndex(src, value, index) => QC inj Map(src, Free roll MapFuncs.ProjectIndex(value, index))
    }
  )

  def QScriptCore[G[_]](implicit QC: QScriptCore :<: G) = make(
    λ[QScriptCore ~> G](fa =>
      QC inj (fa match {
        case Union(src, lb, rb) =>
          Union(src, applyToBranch(lb), applyToBranch(rb))
        case Subset(src, lb, sel, rb) =>
          Subset(src, applyToBranch(lb), sel, applyToBranch(rb))
        case _ => fa
      })
    )
  )
  def ThetaJoin[G[_]](implicit TJ: ThetaJoin :<: G) = make(
    λ[ThetaJoin ~> G](tj =>
      TJ inj quasar.qscript.ThetaJoin(
        tj.src,
        applyToBranch(tj.lBranch),
        applyToBranch(tj.rBranch),
        tj.on,
        tj.f,
        tj.combine
      )
    )
  )
  def EquiJoin[G[_]](implicit EJ: EquiJoin :<: G) = make(
    λ[EquiJoin ~> G](ej =>
      EJ inj quasar.qscript.EquiJoin(
        ej.src,
        applyToBranch(ej.lBranch),
        applyToBranch(ej.rBranch),
        ej.lKey,
        ej.rKey,
        ej.f,
        ej.combine
      )
    )
  )
}

// ShowT is needed for debugging
class NormalizableT[T[_[_]] : Recursive : Corecursive : EqualT : ShowT] extends TTypes[T] {
  import Normalizable._
  lazy val opt = new Optimize[T]

  def freeTC(free: FreeQS): FreeQS = {
    freeTransCata[T, QScriptTotal, QScriptTotal, Hole, Hole](free)(
      liftCo(opt.applyToFreeQS[QScriptTotal])
    )
  }

  def freeTCEq(free: FreeQS): Option[FreeQS] = {
    val freeNormalized = freeTC(free)
    (free ≠ freeNormalized).option(freeNormalized)
  }

  def freeMFEq[A: Equal](fm: Free[MapFunc, A]): Option[Free[MapFunc, A]] = {
    val fmNormalized = freeMF[A](fm)
    (fm ≠ fmNormalized).option(fmNormalized)
  }

  def freeMF[A](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    freeTransCata[T, MapFunc, MapFunc, A, A](fm)(MapFunc.normalize[T, A])

  def makeNorm[A, B, C](
    lOrig: A, rOrig: B)(
    left: A => Option[A], right: B => Option[B])(
    f: (A, B) => C):
      Option[C] =
    (left(lOrig), right(rOrig)) match {
      case (None, None) => None
      case (l, r)       => f(l.getOrElse(lOrig), r.getOrElse(rOrig)).some
    }

  def EquiJoin = make(
    λ[EquiJoin ~> (Option ∘ EquiJoin)#λ](ej =>
      (freeTCEq(ej.lBranch), freeTCEq(ej.rBranch), freeMFEq(ej.lKey), freeMFEq(ej.rKey), freeMFEq(ej.combine)) match {
        case (None, None, None, None, None) => None
        case (lBranchNorm, rBranchNorm, lKeyNorm, rKeyNorm, combineNorm) =>
          quasar.qscript.EquiJoin(
            ej.src,
            lBranchNorm.getOrElse(ej.lBranch),
            rBranchNorm.getOrElse(ej.rBranch),
            lKeyNorm.getOrElse(ej.lKey),
            rKeyNorm.getOrElse(ej.rKey),
            ej.f,
            combineNorm.getOrElse(ej.combine)).some
      }))

  def ThetaJoin = make(
    λ[ThetaJoin ~> (Option ∘ ThetaJoin)#λ](tj =>
      (freeTCEq(tj.lBranch), freeTCEq(tj.rBranch), freeMFEq(tj.on), freeMFEq(tj.combine)) match {
        case (None, None, None, None) => None
        case (lBranchNorm, rBranchNorm, onNorm, combineNorm) =>
          quasar.qscript.ThetaJoin(
            tj.src,
            lBranchNorm.getOrElse(tj.lBranch),
            rBranchNorm.getOrElse(tj.rBranch),
            onNorm.getOrElse(tj.on),
            tj.f,
            combineNorm.getOrElse(tj.combine)).some
      }))

  def QScriptCore = {
    // NB: all single-bucket reductions should reduce on `null`
    def normalizeBucket(bucket: FreeMap[T]): FreeMap[T] = bucket.resume.fold({
      case MapFuncs.Constant(_) => MapFuncs.NullLit[T, Hole]()
      case _                    => bucket
    }, κ(bucket))

    make(λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
      case Reduce(src, bucket, reducers, repair) => {
        val reducersOpt: List[Option[ReduceFunc[FreeMap[T]]]] =
          reducers.map(_.traverse(freeMFEq[Hole](_)))

        val reducersNormOpt: Option[List[ReduceFunc[FreeMap[T]]]] =
          (!reducersOpt.map(_.toList).flatten.isEmpty).option(
            Zip[List].zipWith(reducersOpt, reducers)(_.getOrElse(_)))

        val bucketNormOpt: Option[FreeMap[T]] = freeMFEq(bucket)

        val bucketNormConst: Option[FreeMap[T]] =
          bucketNormOpt.getOrElse(bucket).resume.fold({
            case MapFuncs.Constant(ej) =>
              (!EJson.isNull(ej)).option(MapFuncs.NullLit[T, Hole]())
            case _ => bucketNormOpt
          }, κ(bucketNormOpt))

        (bucketNormConst, reducersNormOpt, freeMFEq(repair)) match {
          case (None, None, None) =>
            None
          case (bucketNorm, reducersNorm, repairNorm)  =>
            Reduce(
              src,
              bucketNorm.getOrElse(bucket),
              reducersNorm.getOrElse(reducers),
              repairNorm.getOrElse(repair)).some
        }
      }

      case Sort(src, bucket, order) => {
        val orderOpt: List[Option[(FreeMap[T], SortDir)]] =
          order.map {
            _.leftMap(freeMFEq(_)) match {
              case (Some(fm), dir) => Some((fm, dir))
              case (_, _)          => None
            }
          }

        val orderNormOpt: Option[List[(FreeMap[T], SortDir)]] =
          (!orderOpt.map(_.toList).flatten.isEmpty).option(
            Zip[List].zipWith(orderOpt, order)(_.getOrElse(_)))

        makeNorm(bucket, order)(freeMFEq(_), _ => orderNormOpt)(Sort(src, _, _))
      }
      case Map(src, f)            => freeMFEq(f).map(Map(src, _))
      case LeftShift(src, s, r)   => makeNorm(s, r)(freeMFEq(_), freeMFEq(_))(LeftShift(src, _, _))
      case Union(src, l, r)       => makeNorm(l, r)(freeTCEq(_), freeTCEq(_))(Union(src, _, _))
      case Filter(src, f)         => freeMFEq(f).map(Filter(src, _))
      case Subset(src, from, sel, count) => makeNorm(from, count)(freeTCEq(_), freeTCEq(_))(Subset(src, _, sel, _))
      case Unreferenced()         => None
    })
  }

  def ProjectBucket = make(
    λ[ProjectBucket ~> (Option ∘ ProjectBucket)#λ] {
      case BucketField(a, v, f) => makeNorm(v, f)(freeMFEq(_), freeMFEq(_))(BucketField(a, _, _))
      case BucketIndex(a, v, i) => makeNorm(v, i)(freeMFEq(_), freeMFEq(_))(BucketIndex(a, _, _))
    }
  )
}
