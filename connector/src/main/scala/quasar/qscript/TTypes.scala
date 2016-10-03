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

import quasar.Predef._
import quasar.fp._
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
}

object TTypes {
  def normalizable[T[_[_]] : Recursive : Corecursive : EqualT : ShowT] = new NormalizableT[T]
  def simplifiableProjection[T[_[_]]]                                  = new SimplifiableProjectionT[T]
}

class SimplifiableProjectionT[T[_[_]]] extends TTypes[T] {
  import SimplifyProjection._

  private lazy val simplify: EndoK[QScriptTotal] = simplifyQScriptTotal[T].simplifyProjection
  private def applyToBranch(branch: FreeQS[T]): FreeQS[T] = branch mapSuspension simplify

  def ProjectBucket[G[_]](implicit QC: QScriptCore :<: G) = make(
    λ[ProjectBucket ~> G] {
      case BucketField(src, value, field) => QC inj Map(src, Free roll MapFuncs.ProjectField(value, field))
      case BucketIndex(src, value, index) => QC inj Map(src, Free roll MapFuncs.ProjectIndex(value, index))
    }
  )

  def QScriptCore[G[_]](implicit QC: QScriptCore :<: G) = make(
    λ[QScriptCore ~> G](fa =>
      QC inj (fa match {
        case Union(src, lb, rb) => Union(src, applyToBranch(lb), applyToBranch(rb))
        case Drop(src, lb, rb)  => Drop(src, applyToBranch(lb), applyToBranch(rb))
        case Take(src, lb, rb)  => Take(src, applyToBranch(lb), applyToBranch(rb))
        case _                  => fa
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

  def freeTC(free: FreeQS[T]): FreeQS[T] = {
    freeTransCata[T, QScriptTotal, QScriptTotal, Hole, Hole](free)(
      liftCo(opt.applyToFreeQS[QScriptTotal])
    )
  }
  def freeMF[A](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    freeTransCata[T, MapFunc, MapFunc, A, A](fm)(MapFunc.normalize[T, A])

  def EquiJoin = make(
    λ[EndoK[EquiJoin]](ej =>
      quasar.qscript.EquiJoin(
        ej.src,
        freeTC(ej.lBranch),
        freeTC(ej.rBranch),
        freeMF(ej.lKey),
        freeMF(ej.rKey),
        ej.f,
        freeMF(ej.combine)
      )
    )
  )
  def ThetaJoin = make(
    λ[EndoK[ThetaJoin]](tj =>
      quasar.qscript.ThetaJoin(
        tj.src,
        freeTC(tj.lBranch),
        freeTC(tj.rBranch),
        freeMF(tj.on),
        tj.f,
        freeMF(tj.combine)
      )
    )
  )

  def QScriptCore = {
    // NB: all single-bucket reductions should reduce on `null`
    def normalizeBucket(bucket: FreeMap[T]): FreeMap[T] = bucket.resume.fold({
      case MapFuncs.Constant(_) => MapFuncs.NullLit[T, Hole]()
      case _                    => bucket
    }, κ(bucket))

    make(λ[EndoK[QScriptCore]] {
      case Map(src, f)                           => Map(src, freeMF(f))
      case LeftShift(src, s, r)                  => LeftShift(src, freeMF(s), freeMF(r))
      case Reduce(src, bucket, reducers, repair) => Reduce(src, normalizeBucket(freeMF(bucket)), reducers map (_ map freeMF), freeMF(repair))
      case Sort(src, bucket, order)              => Sort(src, freeMF(bucket), order map (_ leftMap freeMF))
      case Union(src, l, r)                      => Union(src, freeTC(l), freeTC(r))
      case Filter(src, f)                        => Filter(src, freeMF(f))
      case Take(src, from, count)                => Take(src, freeTC(from), freeTC(count))
      case Drop(src, from, count)                => Drop(src, freeTC(from), freeTC(count))
      case Unreferenced()                        => Unreferenced()
    })
  }

  def ProjectBucket = make(
    λ[EndoK[ProjectBucket]] {
      case BucketField(a, v, f) => BucketField(a, freeMF(v), freeMF(f))
      case BucketIndex(a, v, i) => BucketField(a, freeMF(v), freeMF(i))
    }
  )
}
