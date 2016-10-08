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
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.fp.ski._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._

import matryoshka._, Recursive.ops._
import matryoshka.patterns._
import scalaz._, Scalaz._

/** Rewrites adjacent nodes. */
trait Coalesce[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  /** Coalesce for types containing QScriptCore. */
  def coalesceQC[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit QC: QScriptCore[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  /** Coalesce for types containing ShiftedRead. */
  def coalesceSR[F[_]: Functor]
    (FToOut: PrismNT[F, OUT])
    (implicit SR: Const[ShiftedRead, ?] :<: OUT)
      : IN[IT[F]] => Option[IN[IT[F]]]

  /** Coalesce for types containing ThetaJoin. */
  def coalesceTJ[F[_]: Functor]
    (FToOut: F ~> λ[α => Option[OUT[α]]])
    (implicit TJ: ThetaJoin[IT, ?] :<: OUT)
      : IN[IT[F]] => Option[OUT[IT[F]]]
}

object Coalesce {
  type Aux[T[_[_]], IN[_], F[_]] = Coalesce[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: Coalesce.Aux[T, IN, OUT]) = ev

  private def CoalesceTotal[T[_[_]]: Recursive: Corecursive: EqualT] =
    Coalesce[T, QScriptTotal[T, ?], QScriptTotal[T, ?]]

  private def freeQC[T[_[_]]: Recursive: Corecursive: EqualT](branch: FreeQS[T])
      : FreeQS[T] =
    freeTransCata[T, QScriptTotal[T, ?], QScriptTotal[T, ?], Hole, Hole](branch)(co =>
      co.run.fold(
        κ(co),
        in => CoEnv(repeatedly(CoalesceTotal.coalesceQC(coenvPrism[QScriptTotal[T, ?], Hole]))(in).right)))

  private def freeQCEq[T[_[_]]: Recursive: Corecursive: EqualT](branch: FreeQS[T])
      : Option[FreeQS[T]] = {
    val coalesced = freeQC(branch)
    (branch ≠ coalesced).option(coalesced)
  }

  private def freeSR[T[_[_]]: Recursive: Corecursive: EqualT](branch: FreeQS[T])
      : FreeQS[T] =
    freeTransCata[T, QScriptTotal[T, ?], QScriptTotal[T, ?], Hole, Hole](branch)(co =>
      co.run.fold(
        κ(co),
        in => CoEnv(repeatedly(CoalesceTotal.coalesceSR(coenvPrism[QScriptTotal[T, ?], Hole]))(in).right)))

  private def freeSREq[T[_[_]]: Recursive: Corecursive: EqualT](branch: FreeQS[T])
      : Option[FreeQS[T]] = {
    val coalesced = freeSR(branch)
    (branch ≠ coalesced).option(coalesced)
  }

  private def freeTJ[T[_[_]]: Recursive: Corecursive: EqualT](branch: FreeQS[T])
      : FreeQS[T] =
    freeTransCata[T, QScriptTotal[T, ?], QScriptTotal[T, ?], Hole, Hole](branch)(co =>
      co.run.fold(
        κ(co),
        in => CoEnv(repeatedly(CoalesceTotal.coalesceTJ(coenvPrism[QScriptTotal[T, ?], Hole].get))(in).right)))

  private def freeTJEq[T[_[_]]: Recursive: Corecursive: EqualT](branch: FreeQS[T])
      : Option[FreeQS[T]] = {
    val coalesced = freeTJ(branch)
    (branch ≠ coalesced).option(coalesced)
  }

  private def makeBranched[A, B]
    (lOrig: A, rOrig: A)
    (op: A => Option[A])
    (f: (A, A) => B)
      : Option[B] =
    (op(lOrig), op(rOrig)) match {
      case (None, None) => None
      case (l,    r)    => f(l.getOrElse(lOrig), r.getOrElse(rOrig)).some
    }

  def rewrite[T[_[_]]: Recursive: Corecursive: EqualT](elem0: FreeMap[T])
      : Option[FreeMap[T]] = {
    val elem: T[CoEnv[Hole, MapFunc[T, ?], ?]] = elem0.toCoEnv[T]

    val hole: T[CoEnv[Hole, MapFunc[T, ?], ?]] = HoleF.toCoEnv[T]

    val oneRef =
      Free.roll[MapFunc[T, ?], Hole](ProjectIndex(HoleF, IntLit(1))).toCoEnv[T]
    val rightCount: Int = elem.para(count(hole))

    // all `RightSide` access is through `oneRef`
    (elem.para(count(oneRef)) ≟ rightCount).option(
      transApoT(elem)(substitute(oneRef, hole)).fromCoEnv)
  }

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive: EqualT, G[_]]
    (implicit QC: QScriptCore[T, ?] :<: G)
      : Coalesce.Aux[T, QScriptCore[T, ?], G] =
    new Coalesce[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      // TODO: I feel like this must be some standard fold.
      def sequenceReduce[T[_[_]]: EqualT](rf: ReduceFunc[(FreeMap[T], JoinFunc[T])])
          : Option[(FreeMap[T], ReduceFunc[JoinFunc[T]])] =
        rf match {
          case ReduceFuncs.Count(a)           => (a._1, ReduceFuncs.Count(a._2)).some
          case ReduceFuncs.Sum(a)             => (a._1, ReduceFuncs.Sum(a._2)).some
          case ReduceFuncs.Min(a)             => (a._1, ReduceFuncs.Min(a._2)).some
          case ReduceFuncs.Max(a)             => (a._1, ReduceFuncs.Max(a._2)).some
          case ReduceFuncs.Avg(a)             => (a._1, ReduceFuncs.Avg(a._2)).some
          case ReduceFuncs.Arbitrary(a)       => (a._1, ReduceFuncs.Arbitrary(a._2)).some
          case ReduceFuncs.UnshiftArray(a)    => (a._1, ReduceFuncs.UnshiftArray(a._2)).some
          case ReduceFuncs.UnshiftMap(a1, a2) =>
            (a1._1 ≟ a2._1).option((a1._1, ReduceFuncs.UnshiftMap(a1._2, a2._2)))
        }

      def rightOnly(replacement: FreeMap[T])
          : JoinFunc[T] => Option[FreeMap[T]] =
        _.traverseM[Option, Hole] {
          case LeftSide  => None
          case RightSide => replacement.some
        }

      def coalesceQC[F[_]: Functor]
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
            case Drop(innerSrc, lb, rb) =>
              Drop(innerSrc,
                Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(lb, mf))),
                rb).some
            case Take(innerSrc, lb, rb) =>
              Take(innerSrc,
                Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(lb, mf))),
                rb).some
            case _ => None
          })
        case LeftShift(Embed(src), struct, shiftRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
            case Map(innerSrc, mf) if !shiftRepair.element(LeftSide) =>
              LeftShift(innerSrc, struct >> mf, shiftRepair).some
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftArray(elem)), redRepair)
                if freeTransCata(struct >> redRepair)(MapFunc.normalize) ≟ Free.point(ReduceIndex(0)) =>
              rightOnly(elem)(shiftRepair) ∘ (Map(srcInner, _))
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftMap(_, elem)), redRepair)
                if freeTransCata(struct >> redRepair)(MapFunc.normalize) ≟ Free.point(ReduceIndex(0)) =>
              rightOnly(elem)(shiftRepair) ∘ (Map(srcInner, _))
            case Reduce(srcInner, _, List(ReduceFuncs.UnshiftMap(k, elem)), redRepair)
                if freeTransCata(struct >> redRepair)(MapFunc.normalize) ≟ Free.roll(ZipMapKeys(Free.point(ReduceIndex(0)))) =>
              rightOnly(
                Free.roll(ConcatArrays[T, FreeMap[T]](Free.roll(MakeArray(k)), Free.roll(MakeArray(elem)))))(
                shiftRepair) ∘
                (Map(srcInner, _))
            case _ => None
          }
        case Reduce(Embed(src), bucket, reducers, redRepair) =>
          FToOut.get(src) >>= QC.prj >>= {
            case LeftShift(innerSrc, struct, shiftRepair)
                if shiftRepair =/= RightSideF =>
              (rightOnly(HoleF)(freeTransCata(bucket >> shiftRepair)(MapFunc.normalize)) ⊛
                reducers.traverse(_.traverse(mf => rightOnly(HoleF)(freeTransCata(mf >> shiftRepair)(MapFunc.normalize)))))((b, r) =>
                Reduce(FToOut.reverseGet(QC.inj(LeftShift(innerSrc, struct, RightSideF))).embed, b, r, redRepair))
            case LeftShift(innerSrc, struct, shiftRepair) =>
              (rewriteShift(struct, freeTransCata(bucket >> shiftRepair)(MapFunc.normalize)) ⊛
                reducers.traverse(_.traverse(mf => rewriteShift(struct, freeTransCata(mf >> shiftRepair)(MapFunc.normalize)))))((b, r) =>
                r.foldRightM[Option, (FreeMap[T], (JoinFunc[T], List[ReduceFunc[JoinFunc[T]]]))]((b._1, (b._2, Nil)))((elem, acc) => {
                  sequenceReduce(elem) >>= (e =>
                    (e._1 ≟ acc._1).option(
                      (acc._1, (acc._2._1, e._2 :: acc._2._2))))
                })).join >>= {
                case (st, (bucket, reducers)) =>
                  if (st ≟ struct) None
                  else
                    (rightOnly(HoleF)(bucket) ⊛
                      (reducers.traverse(_.traverse(rightOnly(HoleF)))))((sb, sr) =>
                      Reduce(FToOut.reverseGet(QC.inj(LeftShift(innerSrc, st, RightSideF))).embed, sb, sr, redRepair))
              }
            case Map(innerSrc, mf) =>
              Reduce(
                innerSrc,
                freeTransCata(bucket >> mf)(MapFunc.normalize),
                reducers.map(_.map(red => freeTransCata(red >> mf)(MapFunc.normalize))),
                redRepair).some
            case _ => None
          }
        case Filter(Embed(src), cond) => FToOut.get(src) >>= QC.prj >>= {
          case Filter(srcInner, condInner) =>
            Filter(srcInner, Free.roll[MapFunc[T, ?], Hole](And(condInner, cond))).some
          case _ => None
        }
        case Drop(src, from, count) =>
          makeBranched(from, count)(freeQCEq[T])(Drop(src, _, _))
        case Take(src, from, count) =>
          makeBranched(from, count)(freeQCEq[T])(Take(src, _, _))
        case Union(src, from, count) =>
          makeBranched(from, count)(freeQCEq[T])(Union(src, _, _))
        case _ => None
      }

      def coalesceSR[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit SR: Const[ShiftedRead, ?] :<: OUT) = {
        case Map(Embed(src), mf) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ rewrite(mf))((const, newMF) =>
            Map(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead, T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
              newMF))
        case Reduce(Embed(src), bucket, reducers, repair) =>
          ((FToOut.get(src) >>= SR.prj) ⊛ rewrite(bucket) ⊛ reducers.traverse(_.traverse(rewrite[T])))(
            (const, newBuck, newRed) =>
            Reduce(
              FToOut.reverseGet(SR.inj(Const[ShiftedRead, T[F]](ShiftedRead(const.getConst.path, ExcludeId)))).embed,
              newBuck,
              newRed,
              repair))
        case Drop(src, from, count) =>
          makeBranched(from, count)(freeSREq[T])(Drop(src, _, _))
        case Take(src, from, count) =>
          makeBranched(from, count)(freeSREq[T])(Take(src, _, _))
        case Union(src, from, count) =>
          makeBranched(from, count)(freeSREq[T])(Union(src, _, _))
        case _ => None
      }

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) = {
        case Map(Embed(src), mf) =>
          (FToOut(src) >>= TJ.prj).map(
            tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc[T]))(tj)))
        case Drop(src, from, count) =>
          makeBranched(from, count)(freeTJEq[T])((l, r) => QC.inj(Drop(src, l, r)))
        case Take(src, from, count) =>
          makeBranched(from, count)(freeTJEq[T])((l, r) => QC.inj(Take(src, l, r)))
        case Union(src, from, count) =>
          makeBranched(from, count)(freeTJEq[T])((l, r) => QC.inj(Union(src, l, r)))
        case _ => None
      }
    }

  implicit def projectBucket[T[_[_]]: Recursive, F[_]]
      : Coalesce.Aux[T, ProjectBucket[T, ?], F] =
    new Coalesce[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def coalesceQC[F[_]: Functor]
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

      def coalesceSR[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit SR: Const[ShiftedRead, ?] :<: OUT) =
        κ(None)

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) =
        κ(None)
    }

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: Coalesce.Aux[T, F, H], G: Coalesce.Aux[T, G, H])
      : Coalesce.Aux[T, Coproduct[F, G, ?], H] =
    new Coalesce[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore[IT, ?] :<: OUT) =
        _.run.bitraverse(F.coalesceQC(FToOut), G.coalesceQC(FToOut)) ∘ (Coproduct(_))

      def coalesceSR[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit SR: Const[ShiftedRead, ?] :<: OUT) =
        _.run.bitraverse(F.coalesceSR(FToOut), G.coalesceSR(FToOut)) ∘ (Coproduct(_))

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) =
        _.run.fold(F.coalesceTJ(FToOut), G.coalesceTJ(FToOut))
    }

  def default[T[_[_]], IN[_], G[_]]: Coalesce.Aux[T, IN, G] =
    new Coalesce[IN] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def coalesceQC[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit QC: QScriptCore [IT, ?] :<: OUT) =
        κ(None)

      def coalesceSR[F[_]: Functor]
        (FToOut: PrismNT[F, OUT])
        (implicit SR: Const[ShiftedRead, ?] :<: OUT) =
        κ(None)

      def coalesceTJ[F[_]: Functor]
        (FToOut: F ~> λ[α => Option[OUT[α]]])
        (implicit TJ: ThetaJoin[IT, ?] :<: OUT) =
        κ(None)
    }

  implicit def deadEnd[T[_[_]], OUT[_]]: Coalesce.Aux[T, Const[DeadEnd, ?], OUT] =
    default

  implicit def read[T[_[_]], OUT[_]]: Coalesce.Aux[T, Const[Read, ?], OUT] =
    default

  implicit def shiftedRead[T[_[_]], OUT[_]]
      : Coalesce.Aux[T, Const[ShiftedRead, ?], OUT] =
    default

  implicit def thetaJoin[T[_[_]], OUT[_]]: Coalesce.Aux[T, ThetaJoin[T, ?], OUT] =
    default

  implicit def equiJoin[T[_[_]], OUT[_]]: Coalesce.Aux[T, EquiJoin[T, ?], OUT] =
    default
}
