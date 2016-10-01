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
import quasar.SKI._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._

import matryoshka._
import scalaz._, Scalaz._

/** This will apply any rewrites to IN when preceded by a [[QScriptCore]] node,
  * where the resulting node is in `IN`.
  */
trait Coalesce[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def coalesce[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore [IT, ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]
}

object Coalesce {
  type Aux[T[_[_]], IN[_], F[_]] = Coalesce[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: Coalesce.Aux[T, IN, OUT]) = ev

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive: EqualT, G[_]]
      : Coalesce.Aux[T, QScriptCore[T, ?], G] =
    new Coalesce[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      val FI = scala.Predef.implicitly[Injectable.Aux[QScriptCore[T, ?], QScriptTotal[T, ?]]]

      def coalesce[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore [IT, ?] :<: OUT) = {
        case Map(Embed(src), mf) => FToOut.get(src) >>= QC.prj >>= (s =>
          if (mf.length ≟ 0 && (s match { case Unreferenced() => false; case _ => true }))
            Map(
              FToOut.reverseGet(QC.inj(Unreferenced[T, T[F]]())).embed,
              mf).some
          else s match {
            case Map(srcInner, mfInner) => Map(srcInner, mf >> mfInner).some
            case LeftShift(srcInner, struct, repair) =>
              LeftShift(srcInner, struct, mf >> repair).some
            case Reduce(srcInner, bucket, funcs, repair) =>
              Reduce(srcInner, bucket, funcs, mf >> repair).some
            case _ => None
          })
        case LeftShift(Embed(src), struct, shiftRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
            case Map(innerSrc, mf) if !shiftRepair.element(LeftSide) =>
              LeftShift(innerSrc, struct >> mf, shiftRepair).some
            // TODO: I think this can capture cases where the LS.struct and
            //       Reduce.repair are more complex. Or maybe that’s already
            //       simplified by normalization?
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftArray(elem)), redRepair)
                if struct ≟ HoleF && redRepair ≟ Free.point(ReduceIndex(0)) =>
              shiftRepair.traverseM {
                case LeftSide => None
                case RightSide => elem.some
              } ∘ (Map(srcInner, _))
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftMap(_, elem)), redRepair)
                if struct ≟ HoleF && redRepair ≟ Free.point(ReduceIndex(0)) =>
              shiftRepair.traverseM {
                case LeftSide => None
                case RightSide => elem.some
              } ∘ (Map(srcInner, _))
            case _ => None
          }
        // TODO: For Take and Drop, we should be able to pull _most_ of a Reduce repair function to after T/D
        case Take(src, from, count) => // Pull more work to _after_ limiting the dataset
          from.resume.swap.toOption >>= FI.project >>= {
            case Map(fromInner, mf) => Map(FToOut.reverseGet(QC.inj(Take(src, fromInner, count))).embed, mf).some
            case _ => None
          }
        case Drop(src, from, count) => // Pull more work to _after_ limiting the dataset
          from.resume.swap.toOption >>= FI.project >>= {
            case Map(fromInner, mf) => Map(FToOut.reverseGet(QC.inj(Drop(src, fromInner, count))).embed, mf).some
            case _ => None
          }
        case Filter(Embed(src), cond) => FToOut.get(src) >>= QC.prj >>= {
          case Filter(srcInner, condInner) =>
            Filter(srcInner, Free.roll[MapFunc[T, ?], Hole](And(condInner, cond))).some
          case _ => None
        }
        case _ => None
      }
    }

  implicit def projectBucket[T[_[_]]: Recursive, F[_]]
      : Coalesce.Aux[T, ProjectBucket[T, ?], F] =
    new Coalesce[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def coalesce[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT) = {
        case BucketField(Embed(src), value, field) => FToOut.get(src) >>= QC.prj >>= {
          case Map(srcInner, mf) =>
            BucketField(srcInner, value >> mf, field >> mf).some
          case _ => None
        }
        case BucketIndex(Embed(src), value, index) => FToOut.get(src) >>= QC.prj >>= {
          case Map(srcInner, mf) =>
            BucketIndex(srcInner, value >> mf, index >> mf).some
          case _ => None
        }
      }
    }

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: Coalesce.Aux[T, F, H], G: Coalesce.Aux[T, G, H])
      : Coalesce.Aux[T, Coproduct[F, G, ?], H] =
    new Coalesce[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def coalesce[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT) =
        _.run.bitraverse(F.coalesce(FToOut), G.coalesce(FToOut)) ∘ (Coproduct(_))
    }

  def default[T[_[_]], F[_], G[_]]: Coalesce.Aux[T, F, G] =
    new Coalesce[F] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesce[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore [IT, ?] :<: OUT) =
        κ(None)
    }

  implicit def deadEnd[T[_[_]], G[_]]: Coalesce.Aux[T, Const[DeadEnd, ?], G] =
    default

  implicit def read[T[_[_]], G[_]]: Coalesce.Aux[T, Const[Read, ?], G] =
    default

  implicit def shiftedRead[T[_[_]], G[_]]
      : Coalesce.Aux[T, Const[ShiftedRead, ?], G] =
    default

  implicit def thetaJoin[T[_[_]], G[_]]: Coalesce.Aux[T, ThetaJoin[T, ?], G] =
    default

  implicit def equiJoin[T[_[_]], G[_]]: Coalesce.Aux[T, EquiJoin[T, ?], G] =
    default
}
