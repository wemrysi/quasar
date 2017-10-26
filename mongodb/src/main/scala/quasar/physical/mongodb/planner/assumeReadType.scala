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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar._, Planner._, Type.{Const => _, _}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fs.{FileSystemError, MonadFsErr}, FileSystemError.qscriptPlanningFailed
import quasar.physical.mongodb.planner.common._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz.{ToIdOps => _, _}

object assumeReadType {

  object FreeShiftedRead {
    def unapply[T[_[_]]](fq: FreeQS[T])(
      implicit QSR: Const[ShiftedRead[AFile], ?] :<: QScriptTotal[T, ?]
    ) : Option[ShiftedRead[AFile]] = fq match {
      case Embed(CoEnv(\/-(QSR(qsr: Const[ShiftedRead[AFile], _])))) => qsr.getConst.some
      case _ => none
    }
  }

  def isRewrite[T[_[_]]: BirecursiveT: EqualT, F[_]: Functor, G[_]: Functor, A](GtoF: PrismNT[G, F], qs: G[A])(implicit
    QC: QScriptCore[T, ?] :<: F,
    SR: Const[ShiftedRead[AFile], ?] :<: F,
    ev: Recursive.Aux[A, G])
      : Boolean = qs match {
    case GtoF(QC(Filter(Embed(GtoF(SR(Const(ShiftedRead(_, ExcludeId))))), _)))
       | GtoF(QC(Sort(Embed(GtoF(SR(Const(ShiftedRead(_, ExcludeId))))), _, _)))
       | GtoF(QC(Sort(Embed(GtoF(QC(Filter(Embed(GtoF(SR(Const(ShiftedRead(_, ExcludeId))))), _)))), _ , _)))
       | GtoF(QC(Subset(_, FreeShiftedRead(ShiftedRead(_, ExcludeId)), _ , _)))
       | GtoF(SR(Const(ShiftedRead(_, ExcludeId)))) => true
    case _ => false
  }

  def elideMoreGeneralGuards[M[_]: Applicative: MonadFsErr, T[_[_]]: RecursiveT]
    (subType: Type)
      : CoEnvMap[T, FreeMap[T]] => M[CoEnvMap[T, FreeMap[T]]] = {
    def f: CoEnvMap[T, FreeMap[T]] => M[CoEnvMap[T, FreeMap[T]]] = {
      case free @ CoEnv(\/-(MFC(MapFuncsCore.Guard(Embed(CoEnv(-\/(SrcHole))), typ, cont, fb)))) =>
        if (typ.contains(subType)) cont.project.point[M]
        // TODO: Error if there is no overlap between the types.
        else {
          val union = subType ⨯ typ
          if (union ≟ Type.Bottom)
            raiseErr(qscriptPlanningFailed(InternalError.fromMsg(s"can only contain ${subType.shows}, but a(n) ${typ.shows} is expected")))
          else {
            CoEnv[Hole, MapFunc[T, ?], FreeMap[T]](MFC(MapFuncsCore.Guard[T, FreeMap[T]](HoleF[T], union, cont, fb)).right).point[M]
          }
        }
      case x => x.point[M]
    }
    f
  }

def apply[T[_[_]]: BirecursiveT: EqualT, F[_]: Functor, M[_]: Monad: MonadFsErr]
  (typ: Type)
  (implicit
    QC: QScriptCore[T, ?] :<: F,
    SR: Const[ShiftedRead[AFile], ?] :<: F)
    : TransM[F, M] =
  new TransM[F, M] {

    def trans[A, G[_]: Functor]
      (GtoF: PrismNT[G, F])
      (implicit TC: Corecursive.Aux[A, G], TR: Recursive.Aux[A, G])
        : F[A] => M[G[A]] = {
        case f @ QC(Filter(src, cond)) =>
          if (isRewrite[T, F, G, A](GtoF, src.project)) {
            ((MapFuncCore.flattenAnd(cond))
              .traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ))))
              .map(_.toList.filter {
                case MapFuncsCore.BoolLit(true) => false
                case _ => true
              } match {
                case Nil => src.project
                case h :: t => GtoF.reverseGet(
                  QC(Filter(src, t.foldLeft[FreeMap[T]](h)((acc, e) => Free.roll(MFC(MapFuncsCore.And(acc, e)))))))
              })
          } else
            GtoF.reverseGet(f).point[M]
        case ls @ QC(LeftShift(src, struct, id, repair)) =>
          if (isRewrite[T, F, G, A](GtoF, src.project)) {
            struct.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
            (struct => GtoF.reverseGet(QC(LeftShift(src, struct, id, repair))))
          } else
            GtoF.reverseGet(ls).point[M]
        case m @ QC(qscript.Map(src, mf)) =>
          if (isRewrite[T, F, G, A](GtoF, src.project)) {
            mf.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
            (mf => GtoF.reverseGet(QC(qscript.Map(src, mf))))
          } else
            GtoF.reverseGet(m).point[M]
        case r @ QC(Reduce(src, b, red, rep)) =>
          if (isRewrite[T, F, G, A](GtoF, src.project)) {
            (b.traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ))) ⊛
              red.traverse(_.traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ)))))(
              (b, red) => GtoF.reverseGet(QC(Reduce(src, b, red, rep))))
          } else
            GtoF.reverseGet(r).point[M]
        case qc =>
          GtoF.reverseGet(qc).point[M]
      }
  }
}
