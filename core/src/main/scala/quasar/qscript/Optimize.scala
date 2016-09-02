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
import quasar.fs.FileSystemError
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._

import matryoshka._,
  Recursive.ops._,
  FunctorT.ops._,
  TraverseT.nonInheritedOps._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._

// NB: Helper types for `simplifyJoin`.
private final case class EquiJoinKey[T[_[_]]]
  (left: FreeMap[T], right: FreeMap[T])
private final case class SimplifiedJoinCondition[T[_[_]]]
  (keys: List[EquiJoinKey[T]], filter: Option[JoinFunc[T]])

class Optimize[T[_[_]]: Recursive: Corecursive: EqualT: ShowT] {

  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, HoleF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncs
  //       • elideNopJoin ⇒ no `ThetaJoin(???, HoleF, HoleF, LeftSide === RightSide, ???, ???)`
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  // TODO: Turn `elideNop` into a type class?
  // def elideNopFilter[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
  //     QScriptCore[T, T[F]] => F[T[F]] = {
  //   case Filter(src, Patts.True) => src.project
  //   case qc                      => QC.inj(qc)
  // }

  def elideNopMap[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ HoleF => src.project
    case x                          => QC.inj(x)
  }

  def elideNopMapCo[F[_]: Functor, A](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[CoEnv[A, F, ?]]] => CoEnv[A, F, T[CoEnv[A, F, ?]]] = {
    case Map(src, mf) if mf ≟ HoleF => src.project
    case x                          => CoEnv(QC.inj(x).right)
  }

  // FIXME: This really needs to ensure that the condition is that of an
  //        autojoin, otherwise it’ll elide things that are truly meaningful.
  def elideNopJoin[F[_]](
    implicit TJ: ThetaJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F, FI: F :<: QScriptTotal[T, ?]):
      ThetaJoin[T, ?] ~> F =
    new (ThetaJoin[T, ?] ~> F) {
      def apply[A](tj: ThetaJoin[T, A]) = tj match {
        case ThetaJoin(src, l, r, on, Inner, combine)
            if l ≟ Free.point(SrcHole) && r ≟ Free.point(SrcHole) && on ≟ EquiJF =>
          QC.inj(Map(src, combine.map(_ => SrcHole: Hole)))
        case x @ ThetaJoin(src, l, r, on, _, combine) if on ≟ BoolLit(true) =>
          (l.resume.leftMap(_.map(_.resume)), r.resume.leftMap(_.map(_.resume))) match {
            case (-\/(m1), -\/(m2)) => (FI.prj(m1) >>= QC.prj, FI.prj(m2) >>= QC.prj) match {
              case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>
                QC.inj(Map(src, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => mf2
                }))
              case (_, _) => TJ.inj(x)
            }
            case (-\/(m1), \/-(SrcHole)) => (FI.prj(m1) >>= QC.prj) match {
              case Some(Map(\/-(SrcHole), mf1)) =>
                QC.inj(Map(src, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => HoleF
                }))
              case _ => TJ.inj(x)
            }
            case (\/-(SrcHole), -\/(m2)) => (FI.prj(m2) >>= QC.prj) match {
              case Some(Map(\/-(SrcHole), mf2)) =>
                QC.inj(Map(src, combine >>= {
                  case LeftSide  => HoleF
                  case RightSide => mf2
                }))
              case _ => TJ.inj(x)
            }
            case (_, _) => TJ.inj(x)
          }
        case x => TJ.inj(x)
      }
    }

  def rebaseT[F[_]: Traverse](
    target: FreeQS[T])(
    src: T[F])(
    implicit FI: F :<: QScriptTotal[T, ?]):
      Option[T[F]] =
    freeCata[QScriptTotal[T, ?], T[QScriptTotal[T, ?]], T[QScriptTotal[T, ?]]](
      target.map(_ => src.transAna(FI)))(recover(_.embed)).transAnaM(FI.prj)

  def rebaseTCo[F[_]: Traverse](
    target: FreeQS[T])(
    srcCo: T[CoEnv[Hole, F, ?]])(
    implicit FI: F :<: QScriptTotal[T, ?]):
      Option[T[CoEnv[Hole, F, ?]]] =
    // TODO: with the right instances & types everywhere, this should look like
    //       target.transAnaM(_.htraverse(FI.prj)) ∘ (srcCo >> _)
    freeTransCataM[T, Option, QScriptTotal[T, ?], F, Hole, Hole](
      target)(
      coEnvHtraverse(_)(new (QScriptTotal[T, ?] ~> (Option ∘ F)#λ) {
        def apply[A](qt: QScriptTotal[T, A]): Option[F[A]] = FI.prj(qt)
      })).map(targ => (srcCo.fromCoEnv >> targ).toCoEnv[T])

  def elideConstantJoin[F[_], G[_]](
    rebase: FreeQS[T] => T[G] => Option[T[G]])(
    implicit TJ: ThetaJoin[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?]):
      ThetaJoin[T, T[G]] => F[T[G]] = {
    case x @ ThetaJoin(src, l, r, on, Inner, combine) if on ≟ BoolLit(true) =>
      (l.resume.leftMap(_.map(_.resume)), r.resume.leftMap(_.map(_.resume))) match {
        case (-\/(m1), -\/(m2)) => (FI.prj(m1) >>= QC.prj, FI.prj(m2) >>= QC.prj) match {
          case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>  // both sides are a Map
            (mf1.resume, mf2.resume) match { // if both sides are Constant, we hit the first case
              case (-\/(Constant(_)), _) =>
                rebase(r)(src).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => HoleF
                }))).getOrElse(TJ.inj(x))
              case (_, -\/(Constant(_))) =>
                rebase(l)(src).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => HoleF
                  case RightSide => mf2
                }))).getOrElse(TJ.inj(x))
              case (_, _) => TJ.inj(x)
            }
          case (Some(Map(\/-(SrcHole), mf1)), _) =>  // left side is a Map
            mf1.resume match {
              case -\/(Constant(_)) =>
                rebase(r)(src).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => HoleF
                }))).getOrElse(TJ.inj(x))
              case _ => TJ.inj(x)
            }
          case (_, Some(Map(\/-(SrcHole), mf2))) =>  // right side is a Map
            mf2.resume match {
              case -\/(Constant(_)) =>
                rebase(l)(src).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => HoleF
                  case RightSide => mf2
                }))).getOrElse(TJ.inj(x))
              case _ => TJ.inj(x)
            }
          case (_, _)=> TJ.inj(x)
        }
        case (_, _) => TJ.inj(x)
      }
    case x => TJ.inj(x)
  }

  def simplifyProjection:
      ProjectBucket[T, ?] ~> QScriptCore[T, ?] =
    new (ProjectBucket[T, ?] ~> QScriptCore[T, ?]) {
      def apply[A](proj: ProjectBucket[T, A]) = proj match {
        case BucketField(src, value, field) =>
          Map(src, Free.roll(MapFuncs.ProjectField(value, field)))
        case BucketIndex(src, value, index) =>
          Map(src, Free.roll(MapFuncs.ProjectIndex(value, index)))
      }
    }

  /** Replaces [[ThetaJoin]] with [[EquiJoin]], which is often more feasible for
    * connectors to implement. It potentially adds a [[Filter]] iff there are
    * conditions in the [[ThetaJoin]] that can not be handled by an
    * [[EquiJoin]].
    */
  def simplifyJoin[F[_]: Functor]
    (implicit EJ: EquiJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      ThetaJoin[T, T[F]] => F[T[F]] =
    tj => {
      // TODO: This can potentially rewrite conditions to try to get left and right
      //       references on distinct sides.
      def alignCondition(l: JoinFunc[T], r: JoinFunc[T]): Option[EquiJoinKey[T]] =
        if (l.element(LeftSide) && r.element(RightSide) &&
          !l.element(RightSide) && !r.element(LeftSide))
          EquiJoinKey(l.as[Hole](SrcHole), r.as[Hole](SrcHole)).some
        else if (l.element(RightSide) && r.element(LeftSide) &&
          !l.element(LeftSide) && !r.element(RightSide))
          EquiJoinKey(r.as[Hole](SrcHole), l.as[Hole](SrcHole)).some
        else None

      def separateConditions(fm: JoinFunc[T]): SimplifiedJoinCondition[T] =
        fm.resume match {
          case -\/(And(a, b)) =>
            val (fir, sec) = (separateConditions(a), separateConditions(b))
            SimplifiedJoinCondition(
              fir.keys ++ sec.keys,
              fir.filter.fold(
                sec.filter)(
                f => sec.filter.fold(f.some)(s => Free.roll(And[T, JoinFunc[T]](f, s)).some)))
          case -\/(Eq(l, r)) =>
            alignCondition(l, r).fold(
              SimplifiedJoinCondition(Nil, fm.some))(
              pair => SimplifiedJoinCondition(List(pair), None))
          case _ => SimplifiedJoinCondition(Nil, fm.some)
        }

      def mergeSides(jf: JoinFunc[T]): FreeMap[T] =
        jf >>= {
          case LeftSide  => Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0)))
          case RightSide => Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1)))
        }

      val SimplifiedJoinCondition(keys, filter) = separateConditions(tj.on)
      QC.inj(Map(filter.foldLeft(
        EJ.inj(EquiJoin(
          tj.src,
          tj.lBranch,
          tj.rBranch,
          ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.left)))),
          ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.right)))),
          tj.f,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(Free.point(LeftSide))),
            Free.roll(MakeArray(Free.point(RightSide))))))).embed)(
        (ej, filt) => QC.inj(Filter(ej, mergeSides(filt))).embed),
        mergeSides(tj.combine)))
    }

  def coalesceQC[F[_]: Functor, G[_]: Functor]
    (GtoF: PrismNT[G, F])
    (implicit
      QC: QScriptCore[T, ?] :<: F,
      FI: F :<: QScriptTotal[T, ?])
      : QScriptCore[T, T[G]] => Option[QScriptCore[T, T[G]]] = {
    case Map(Embed(src), mf) => GtoF.get(src) >>= QC.prj >>= {
      case Map(srcInner, mfInner) => Map(srcInner, mf >> mfInner).some
      case Reduce(srcInner, bucket, funcs, repair) => Reduce(srcInner, bucket, funcs, mf >> repair).some
      case _ => None
    }
    // TODO: For Take and Drop, we should be able to pull _most_ of a Reduce repair function to after T/D
    case Take(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(GtoF.reverseGet(QC.inj(Take(src, fromInner, count))).embed, mf).some
        case _ => None
      }
    case Drop(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(GtoF.reverseGet(QC.inj(Drop(src, fromInner, count))).embed, mf).some
        case _ => None
      }
    case Filter(Embed(src), cond) => GtoF.get(src) >>= QC.prj >>= {
      case Filter(srcInner, condInner) =>
        Filter(srcInner, Free.roll[MapFunc[T, ?], Hole](And(condInner, cond))).some
      case _ => None
    }
    case _ => None
  }

  def coalesceMapShift[F[_], G[_]: Functor](
    GtoF: G ~> λ[α => Option[F[α]]])(
    implicit SP: SourcedPathable[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[G]] => F[T[G]] = {
    case x @ Map(Embed(src), mf) => (GtoF(src) >>= SP.prj >>= {
      case LeftShift(srcInner, struct, repair) =>
        SP.inj(LeftShift(srcInner, struct, mf >> repair)).some
      case _ => None
    }).getOrElse(QC.inj(x))
    case x => QC.inj(x)
  }

  def coalesceMapJoin[F[_], G[_]: Functor](
    GtoF: G ~> λ[α => Option[F[α]]])(
    implicit QC: QScriptCore[T, ?] :<: F, TJ: ThetaJoin[T, ?] :<: F):
      QScriptCore[T, T[G]] => F[T[G]] = {
    case x @ Map(Embed(src), mf) =>
      (GtoF(src) >>= TJ.prj).fold(
        QC.inj(x))(
        tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc[T]))(tj)))
    case x => QC.inj(x)
  }

  def swapMapCount[F[_], G[_]: Functor]
    (GtoF: G ~> (Option ∘ F)#λ)
    (implicit QC: QScriptCore[T, ?] :<: F)
      : QScriptCore[T, T[G]] => QScriptCore[T, T[G]] = {
    case x @ Map(Embed(src), mf) =>
      (GtoF(src) >>= QC.prj).fold[QScriptCore[T, T[G]]] (
        x)(
        {
          case Drop(innerSrc, lb, rb) =>
            Drop(innerSrc,
              Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(lb, mf))),
              rb)
          case Take(innerSrc, lb, rb) =>
            Take(innerSrc,
              Free.roll(Inject[QScriptCore[T, ?], QScriptTotal[T, ?]].inj(Map(lb, mf))),
              rb)
          case _ => x
        })
    case x => x
  }

  def simplifyQC[F[_]: Functor, G[_]: Functor](
    FtoG: F ~> G)(
    implicit DE: Const[DeadEnd, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[G]] => QScriptCore[T, T[G]] = {
    case Map(src, f) if f.length ≟ 0 =>
      Map(FtoG(DE.inj(Const[DeadEnd, T[G]](Root))).embed, f)
    case x => x
  }

  def simplifySP[F[_]: Functor, G[_]: Functor](
    GtoF: G ~> λ[α => Option[F[α]]])(
    implicit SP: SourcedPathable[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      SourcedPathable[T, T[G]] => F[T[G]] = {
    case x @ LeftShift(src, struct, repair) =>
      if (!repair.element(RightSide))
        QC.inj(Map(src, repair ∘ κ(SrcHole)))
      else if (!repair.element(LeftSide))
        (GtoF(src.project) >>= QC.prj >>= {
          case Map(innerSrc, mf) =>
            SP.inj(LeftShift(innerSrc, struct >> mf, repair)).some
          case _ => None
        }).getOrElse(SP.inj(x))
      else
        SP.inj(x)
    case x => SP.inj(x)
  }

  def compactLeftShift[F[_], G[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[G]] => F[T[G]] = {
    case x @ LeftShift(src, struct, repair) => {
      def rewrite(
        src: T[G],
        repair0: JoinFunc[T],
        elem: FreeMap[T],
        dup: FreeMap[T] => Unary[T, FreeMap[T]]):
          F[T[G]] = {
        val repair: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
          repair0.toCoEnv[T]

        val rightSide: JoinFunc[T] =
          Free.point[MapFunc[T, ?], JoinSide](RightSide)
        val rightSideCoEnv: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
          rightSide.toCoEnv[T]

        def makeRef(idx: Int): T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
          Free.roll[MapFunc[T, ?], JoinSide](ProjectIndex(rightSide, IntLit(idx))).toCoEnv[T]

        val zeroRef: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] = makeRef(0)
        val oneRef: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] = makeRef(1)

        val rightCount: Int = repair.para(count(rightSideCoEnv))

        if (repair.para(count(zeroRef)) ≟ rightCount) {   // all `RightSide` access is through `zeroRef`
          val replacement: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
            transApoT(repair)(substitute(zeroRef, rightSideCoEnv))
          SP.inj(LeftShift(src, Free.roll[MapFunc[T, ?], Hole](dup(elem)), replacement.fromCoEnv))
        } else if (repair.para(count(oneRef)) ≟ rightCount) {   // all `RightSide` access is through `oneRef`
          val replacement: T[CoEnv[JoinSide, MapFunc[T, ?], ?]] =
            transApoT(repair)(substitute(oneRef, rightSideCoEnv))
          SP.inj(LeftShift(src, elem, replacement.fromCoEnv))
        } else {
          SP.inj(x)
        }
      }
      struct.resume match {
        case -\/(ZipArrayIndices(elem)) => rewrite(src, repair, elem, fm => DupArrayIndices(fm))
        case -\/(ZipMapKeys(elem)) => rewrite(src, repair, elem, fm => DupMapKeys(fm))
        case _ => SP.inj(x)
      }
    }
    case x => SP.inj(x)
  }

  def compactReduction[F[_]: Functor]:
      QScriptCore[T, T[F]] => QScriptCore[T, T[F]] = {
    case x @ Reduce(src, bucket, reducers0, repair0) => {
      // `reducers`: the reduce funcs that are used
      // `indices`: the indices into `reducers0` that are used
      val (reducers, indices): (List[ReduceFunc[FreeMap[T]]], List[Int]) = {
        val used: Set[Int] = repair0.foldLeft(Set[Int]()) {
          case (acc, redIdx) => acc + redIdx.idx
        }
        reducers0.zipWithIndex.filter {
          case (_, idx) => used.contains(idx)
        }.unzip
      }

      // reset the indices in `repair0`
      val repair: Free[MapFunc[T, ?], Int] = repair0.map {
        case ReduceIndex(idx) => indices.indexOf(idx)
      }

      if (repair.element(-1))
        x
      else
        Reduce(src, bucket, reducers, repair.map(ReduceIndex(_)))
    }
    case x => x
  }

  // TODO: add reordering
  // - Filter can be moved ahead of Sort
  // - Take/Drop can have a normalized order _if_ their counts are constant
  //   (maybe in some additional cases)
  // - Take/Drop can be moved ahead of Map

  // The order of optimizations is roughly this:
  // - elide NOPs
  // - read conversion given to us by the filesystem
  // - convert any remaning projects to maps
  // - coalesce nodes
  // - normalize mapfunc
  def applyAll[F[_]: Traverse: Normalizable](
    implicit DE: Const[DeadEnd, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             SP: SourcedPathable[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?]):
      F[T[F]] => F[T[F]] =
    (Normalizable[F].normalize(_: F[T[F]])) ⋙
      quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      liftFG(elideConstantJoin[F, F](rebaseT[F])) ⋙
      liftFF(repeatedly(coalesceQC[F, F](idPrism))) ⋙
      liftFG(coalesceMapShift[F, F](idPrism.get)) ⋙
      liftFG(coalesceMapJoin[F, F](idPrism.get)) ⋙
      liftFF(simplifyQC[F, F](idPrism.reverseGet)) ⋙
      liftFG(simplifySP[F, F](idPrism.get)) ⋙
      liftFF(swapMapCount[F, F](idPrism.get)) ⋙
      liftFG(compactLeftShift[F, F]) ⋙
      Normalizable[F].normalize ⋙
      liftFF(compactReduction[F]) ⋙
      liftFG(elideNopMap[F])

  def applyToFreeQS[F[_]: Traverse: Normalizable](
    implicit DE: Const[DeadEnd, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             SP: SourcedPathable[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?]):
      F[T[CoEnv[Hole, F, ?]]] => CoEnv[Hole, F, T[CoEnv[Hole, F, ?]]] =
    // FIXME: only apply `simplifyProjection` after pathify
    (quasar.fp.free.injectedNT[F](simplifyProjection).apply(_: F[T[CoEnv[Hole, F, ?]]])) ⋙
      Normalizable[F].normalize ⋙
      quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      liftFG(elideConstantJoin[F, CoEnv[Hole, F, ?]](rebaseTCo[F])) ⋙
      liftFF(repeatedly(coalesceQC[F, CoEnv[Hole, F, ?]](coenvPrism))) ⋙
      liftFG(coalesceMapShift[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      liftFG(coalesceMapJoin[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      // FIXME: currently interferes with elideConstantJoin
      // liftFF(simplifyQC[F, CoEnv[Hole, F, ?]](coenvPrism.reverseGet)) ⋙
      liftFG(simplifySP[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      liftFF(swapMapCount[F, CoEnv[Hole, F, ?]](coenvPrism.get)) ⋙
      liftFG(compactLeftShift[F, CoEnv[Hole, F, ?]]) ⋙
      Normalizable[F].normalize ⋙
      liftFF(compactReduction[CoEnv[Hole, F, ?]]) ⋙
      (fa => QC.prj(fa).fold(CoEnv(fa.right[Hole]))(elideNopMapCo[F, Hole]))  // TODO remove duplication with `elideNopMap`

  /** A backend-or-mount-specific `f` is provided, that allows us to rewrite
    * `Root` (and projections, etc.) into `Read`, so then we can handle exposing
    * only “true” joins and converting intra-data joins to map operations.
    *
    * `f` takes QScript representing a _potential_ path to a file, converts
    * `Root` and its children to path, with the operations post-file remaining.
    */
  def pathify[M[_]: Monad, F[_]: Traverse](
    g: ConvertPath.ListContents[M])(
    implicit FS: StaticPath.Aux[T, F],
             DE: Const[DeadEnd, ?] :<: F,
             F: Pathable[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?],
             CP: ConvertPath.Aux[T, Pathable[T, ?], F]):
      T[F] => EitherT[M,  FileSystemError, T[QScriptTotal[T, ?]]] =
    _.cataM[EitherT[M, FileSystemError, ?], T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]](FS.pathifyƒ[M, F](g)).flatMap(_.fold(qt => EitherT(qt.right.point[M]), FS.toRead[M, F, QScriptTotal[T, ?]](g)))

  def eliminateProjections[M[_]: Monad, F[_]: Traverse](
    fs: Option[ConvertPath.ListContents[M]])(
    implicit FS: StaticPath.Aux[T, F],
             DE: Const[DeadEnd, ?] :<: F,
             F: Pathable[T, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F,
             FI: F :<: QScriptTotal[T, ?],
             CP: ConvertPath.Aux[T, Pathable[T, ?], F]):
      T[F] => EitherT[M, FileSystemError, T[QScriptTotal[T, ?]]] = qs => {
    val res = fs.fold(EitherT(qs.transAna(FI.inj).right[FileSystemError].point[M]))(pathify[M, F](_).apply(qs))

    res.map(
      _.transAna(
        quasar.fp.free.injectedNT[QScriptTotal[T, ?]](simplifyProjection).apply(_: QScriptTotal[T, ?][T[QScriptTotal[T, ?]]])))
  }
}
