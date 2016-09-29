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
}

// ShowT is needed for debugging
class NormalizableT[T[_[_]] : Recursive : Corecursive : EqualT : ShowT] extends TTypes[T] {
  lazy val opt = new Optimize[T]

  def freeTC(free: FreeQS[T]): FreeQS[T] = {
    freeTransCata[T, QScriptTotal, QScriptTotal, Hole, Hole](free)(
      liftCo(opt.applyToFreeQS[QScriptTotal])
    )
  }
  def freeMF[A](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    freeTransCata[T, MapFunc, MapFunc, A, A](fm)(MapFunc.normalize[T, A])

  def EquiJoin = new Normalizable[EquiJoin] {
    val normalize = λ[EndoK[EquiJoin]](ej =>
      quasar.qscript.EquiJoin(
        ej.src,
        freeTC(ej.lBranch),
        freeTC(ej.rBranch),
        freeMF(ej.lKey),
        freeMF(ej.rKey),
        ej.f,
        freeMF(ej.combine))
    )
  }
  def ThetaJoin = new Normalizable[ThetaJoin] {
    val normalize = λ[EndoK[ThetaJoin]](tj =>
      quasar.qscript.ThetaJoin(
        tj.src,
        freeTC(tj.lBranch),
        freeTC(tj.rBranch),
        freeMF(tj.on),
        tj.f,
        freeMF(tj.combine))
    )
  }

  def QScriptCore = new Normalizable[QScriptCore] {
    val normalize = λ[EndoK[QScriptCore]] {
      case Map(src, f)                           => Map(src, freeMF(f))
      case LeftShift(src, s, r)                  => LeftShift(src, freeMF(s), freeMF(r))
      case Reduce(src, bucket, reducers, repair) =>
        val normBuck = freeMF(bucket)
        Reduce(
          src,
          // NB: all single-bucket reductions should reduce on `null`
          normBuck.resume.fold({
            case MapFuncs.Constant(_) => MapFuncs.NullLit[T, Hole]()
            case _                    => normBuck
          }, κ(normBuck)),
          reducers.map(_.map(freeMF(_))),
          freeMF(repair))
      case Sort(src, bucket, order) => Sort(src, freeMF(bucket), order.map(_.leftMap(freeMF(_))))
      case Union(src, l, r)         => Union(src, freeTC(l), freeTC(r))
      case Filter(src, f)           => Filter(src, freeMF(f))
      case Take(src, from, count)   => Take(src, freeTC(from), freeTC(count))
      case Drop(src, from, count)   => Drop(src, freeTC(from), freeTC(count))
      case Unreferenced()           => Unreferenced()
    }
  }

  def ProjectBucket = new Normalizable[ProjectBucket] {
    val normalize = λ[EndoK[ProjectBucket]] {
      case BucketField(a, v, f) => BucketField(a, freeMF(v), freeMF(f))
      case BucketIndex(a, v, i) => BucketField(a, freeMF(v), freeMF(i))
    }
  }
}
