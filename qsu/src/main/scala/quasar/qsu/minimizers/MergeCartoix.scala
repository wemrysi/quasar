/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu
package minimizers

import slamdata.Predef._

import cats.{~>, Eq, Foldable, Monad, Monoid, MonoidK}
import cats.data.Ior
import cats.implicits._

import matryoshka.{Hole => _, _}
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns.interpret

import quasar.{IdStatus, RenderTreeT}
import quasar.RenderTree.ops._
import quasar.common.effect.NameGenerator
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.fp.ski.κ2
import quasar.qscript._
import quasar.qsu.{QScriptUniform => QSU}, QSU.Rotation

import scalaz.{Forall, Foldable1, IList, NonEmptyList}
// these instances don't exist in cats (for good reason), but we depend on them here
import scalaz.std.set.{setMonoid => _, _}
import scalaz.std.map._

import scala.collection.immutable.{Map => SMap}

// An order of magnitude faster to compile than import shims.{plusEmptyToCats => _, _}
import shims.{plusEmptyToCats => _, equalToCats, eqToScalaz, foldableToCats, foldableToScalaz, monadToCats, monadToScalaz, showToCats, traverseToCats}

sealed abstract class MergeCartoix[T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT]
    extends Minimizer[T]
    with MraPhase[T] {

  import IdStatus._
  import MinimizeAutoJoins.MinStateM
  import QSUGraph.Extractors._
  import RecFreeS.RecOps
  import MapFuncsCore.{IntLit, ProjectIndex, ProjectKey, StrLit}

  type Cartouche0 = quasar.qsu.minimizers.Cartouche[T, Index, Hole]
  type Cartouche1 = quasar.qsu.minimizers.Cartouche[T, Nothing, FreeMap]
  type PrjPath = NonEmptyList[Index]
  // TODO: Better names
  type CStage0 = quasar.qsu.minimizers.CStage[T, Index, Hole]
  type CStage1 = quasar.qsu.minimizers.CStage[T, Nothing, FreeMap]
  type ∀[P[_]] = Forall[P]

  implicit def PEqual: Eq[P]

  private val FM = Foldable[FreeMapA]

  private implicit val MM: Monoid[SMap[Symbol, Cartouche0]] =
    MonoidK[SMap[Symbol, ?]].algebra[Cartouche0]

  private val SourceKey = "source"
  private val func = construction.Func[T]

  private def absurd[A]: A = scala.sys.error("absurd!")

  def couldApplyTo(candidates: List[QSUGraph]): Boolean =
    candidates exists { case Transpose(_, _, _) => true; case _ => false }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def extract[
      G[_]: scalaz.Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      candidate: QSUGraph)
      : Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = {

    candidate match {
      // handle a shift candidate
      case Transpose(parent, retain, rotations) =>
        extract[G](parent) map {
          _ map {
            _ andThen {
              _ flatMap { parent2 =>
                updateGraph[G](parent2)(QSU.Transpose(_, retain, rotations))
              }
            }
          }
        }

      // handle a transformed shift candidate
      case MappableRegion.MaximalUnary(Transpose(parent, retain, rotations), fm) =>
        extract[G](parent) map {
          _ map {
            _ andThen { p2 =>
              for {
                parent2 <- p2
                tpose <- updateGraph[G](parent2)(QSU.Transpose(_, retain, rotations))
                res <- updateGraph[G](tpose)(QSU.Map(_, fm.asRec))
              } yield res
            }
          }
        }

      // handle a FreeMap
      case MappableRegion.MaximalUnary(parent, fm) =>
        Some((parent, (parent2, fm2) => {
          val f = MapFuncCore.normalized(fm >> fm2)

          if (f === func.Hole)
            parent2.pure[G]
          else
            updateGraph[G](parent2)(QSU.Map(_, f.asRec))
        }))

      // handle the rest
      case qgraph =>
        Some((qgraph, (src, fm) => {
          if (fm === func.Hole)
            src.pure[G]
          else
            updateGraph[G](src)(QSU.Map(_, fm.asRec))
        }))
    }
  }

  def apply[
      G[_]: scalaz.Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      original: QSUGraph,
      singleSource: QSUGraph,
      candidates0: List[QSUGraph],
      fm0: FreeMapA[Int])
      : G[Option[(QSUGraph, QSUGraph)]] = {

    val (candidates, fm) =
      Some(candidates0)
        .filter(_.exists(_.root === singleSource.root))
        .flatMap(readSourceProjections(singleSource, _, fm0))
        .getOrElse((candidates0.map(Right(_)), fm0))

    val exprProjections: CStage0 => List[CStage0] = {
      case CStage.Expr(f) => quotientProjection(f).toList
      case other => List(other)
    }

    val maybeJoin: Option[CStage.Join[T, Index, Hole]] =
      candidates
        .traverse(_.fold(
          p => Some(p.map(CStage.Project[T, Index](_)).toList),
          readCandidate(singleSource, _)))
        .map { cartoix =>
          val cs = cartoix.zipWithIndex map {
            case (c, i) => Symbol(s"cart$i") -> Cartouche.fromFoldable(c.reverse.flatMap(exprProjections))
          }

          CStage.Join(cs.toMap, fm.map(i => CartoucheRef.Final(Symbol(s"cart$i"))))
        }

    println(s"SINGLE_SOURCE = ${singleSource.root}, candidates = ${candidates0.map(_.root)}\nFM\n${fm0.render.show}")

    maybeJoin.fold(println("NO JOIN"))(j => println(s"MAYBE_JOIN\n${(j: CStage0).render.show}"))

    maybeJoin traverse { j =>
      simplifyJoin[G](j)
        .map { smpl => println(s"SIMPLIFIED_JOIN\n${(smpl: CStage0).render.show}"); smpl }
        .map(finalizeStages)
        .flatMap(j => reifyJoin[G](j, singleSource, StructLens.init(j.cartoix.size > 1), false))
        .map(x => (x, x))
    }
  }

  private final class FuncOf(src: Symbol) {
    def unapply(qgraph: QSUGraph): Option[FreeMap] =
      MappableRegion.unaryOf(src, qgraph).filterNot(_ === func.Hole)
  }

  // returns the list of stages in reverse order, Nil represents the source
  // ignores projection
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def readCandidate(singleSource: QSUGraph, qgraph: QSUGraph): Option[List[CStage0]] = {
    val MapSrc = new FuncOf(singleSource.root)

    qgraph match {
      case Transpose(src, retain, rot) =>
        // struct
        val above: FreeMapA[QSUGraph] =
          MappableRegion[T](singleSource.root === _, src)

        val maybeCartoix: Option[FreeMapA[(Symbol, List[CStage0])]] =
          above.traverse(g => readCandidate(singleSource, g).tupleLeft(g.root))

        maybeCartoix map {
          case FreeA((_, parent)) =>
            CStage.Shift[T, Hole](Hole(), retain.fold(IdOnly, ExcludeId), rot) :: parent

          case cartoix =>
            val roots = FM.foldMap(cartoix) { case (root, _) => Set(root) }

            if (roots.size > 1) {
              val cmap = FM.foldMap(cartoix) {
                case (root, stages) => SMap(root -> Cartouche.fromFoldable(stages.reverse))
              }

              val joiner = cartoix map { case (root, _) => CartoucheRef.Final(root): CartoucheRef }
              val join = CStage.Join(cmap, joiner)
              val current = CStage.Shift[T, Hole](Hole(), retain.fold(IdOnly, ExcludeId), rot)

              // this is an interesting discovery: joins are fully subsuming during read
              // it's justified because we haven't (yet) collapsed above, so there can be
              // no common parents yet. in other words, we read as a tree and collapse to
              // a dag
              current :: join :: Nil
            } else {
              val parent = cartoix.toList.take(1).flatMap { case (_, s) => s }
              val struct = CStage.Expr(cartoix.as(Hole()))
              val current = CStage.Shift[T, Hole](Hole(), retain.fold(IdOnly, ExcludeId), rot)

              struct :: current :: parent
            }
        }

      case MapSrc(f) =>
        Some(List(CStage.Expr(f)))

      case qgraph if qgraph.root === singleSource.root =>
        Some(Nil)

      case _ => None
    }
  }

  private def readSourceProjections(
      singleSource: QSUGraph,
      candidates: List[QSUGraph],
      expr: FreeMapA[Int])
      : Option[(List[Either[PrjPath, QSUGraph]], FreeMapA[Int])] = {

    type T = Either[PrjPath, QSUGraph]

    val len =
      candidates.length

    val indices =
      candidates
        .zipWithIndex
        .flatMap {
          case (c, i) if c.root === singleSource.root => List(i)
          case _ => Nil
        }

    val read =
      Some(attemptProject(expr))
        .filter(_ exists { case (i, p) => indices.contains(i) && p.nonEmpty })
        .map(_ flatMap {
          case (i, h :: t) if indices.contains(i) =>
            FreeA(Left(NonEmptyList.nels(h, t: _*)): T)

          case (i, p) =>
            reifyPath(FreeA(Right(candidates(i)): T), p)
        })

    read map { r =>
      val reindexed = r.zipWithIndex
      (reindexed.toList.sortBy(_._2).map(_._1), reindexed.map(_._2))
    }
  }

  private def attemptProject[A](fm: FreeMapA[A]): FreeMapA[(A, List[Index])] = {
    val fa: A => FreeMapA[(A, List[Index])] =
      a => FreeA((a, Nil))

    val ff: Algebra[MapFunc, FreeMapA[(A, List[Index])]] = {
      case MFC(ProjectKey(FreeA((a, p)), StrLit(key))) =>
        FreeA((a, Index.field(key) :: p))

      case MFC(ProjectIndex(FreeA((a, p)), IntLit(i))) if i.isValidInt =>
        FreeA((a, Index.position(i.toInt) :: p))

      case other =>
        FreeF(other)
    }

    fm.cata(interpret(fa, ff)).map(_.map(_.reverse))
  }

  // Returns an expression where all projections of `Hole` have been
  // extracted, `None` if any access of `Hole` is not a project.
  private def extractProject(fm: FreeMap): Option[FreeMapA[PrjPath]] =
    attemptProject(fm) traverse {
      case (_, h :: t) => Some(NonEmptyList.nels(h, t: _*))
      case _ => None
    }

  private def commonPrjPrefix(xs: PrjPath, ys: PrjPath): Option[PrjPath] = {
    val prefix =
      (xs zip ys).list
        .takeWhile { case (x, y) => x === y }
        .map(_._1)

    prefix.headOption.map(NonEmptyList.nel(_, prefix.drop(1)))
  }

  // Extracts the longest common project prefix of `Hole`, turning it into
  // a `Project` stage, followed by an `Expr` stage containing the remainder
  // of the `FreeMap`.
  private def quotientProjection(fm: FreeMap): NonEmptyList[CStage0] = {
    val factored = for {
      extracted <- extractProject(fm)

      prefix <- extracted.reduceLeftToOption(_.some)((x, y) => x.flatMap(commonPrjPrefix(_, y))).flatten

      len = prefix.size

      expr = extracted.flatMap(p => reifyPath(func.Hole, p.list.drop(len)))
    } yield {
      if (expr === func.Hole)
        prefix.map[CStage0](CStage.Project(_))
      else
        prefix.map[CStage0](CStage.Project(_)) :::> IList(CStage.Expr[T](expr))
    }

    factored getOrElse NonEmptyList[CStage0](CStage.Expr[T](fm))
  }

  private case class Buckets(
      joins: SMap[Symbol, CStage.Join[T, Index, Hole]],
      shifts: SMap[Rotation, SMap[Symbol, CStage.Shift[T, Hole]]],
      projects: SMap[Index, Set[Symbol]],
      exprs: SMap[Symbol, CStage.Expr[T]])

  private object Buckets {
    val Empty = Buckets(SMap(), SMap(), SMap(), SMap())

    /** Bucket each cartouche based on potential compatibility, distinguishing
      * between kind of stage and subgrouping based on attributes (like `Rotation`)
      *
      * Note that being in the same bucket doesn't guarantee compatible, just possible
      */
    def fromCartoix(cartoix: SMap[Symbol, Cartouche0]): Buckets = {
      import CStage._

      cartoix.foldLeft(Empty) {
        case (buckets, (ref, Cartouche.Stages(NonEmptyList(head, _)))) =>
          head match {
            case j @ Join(_, _) =>
              buckets.copy(joins = buckets.joins + (ref -> j))

            case s @ Shift(_, _, rot) =>
              val shiftsMap = buckets.shifts.getOrElse(rot, SMap()) + (ref -> s)
              buckets.copy(shifts = buckets.shifts + (rot -> shiftsMap))

            case Project(p) =>
              val prjsSet = buckets.projects.getOrElse(p, Set()) + ref
              buckets.copy(projects = buckets.projects + (p -> prjsSet))

            case e @ Expr(_) =>
              buckets.copy(exprs = buckets.exprs + (ref -> e))

            case Cartesian(_) => buckets
          }

        case (buckets, _) => buckets
      }
    }
  }

  /** Attempts to simplify a CStage.Join by coalescing compatible stages. */
  @SuppressWarnings(Array(
    "org.wartremover.warts.Recursion",
    "org.wartremover.warts.TraversableOps"))
  private def simplifyJoin[F[_]: Monad: NameGenerator](join: CStage.Join[T, Index, Hole]): F[CStage.Join[T, Index, Hole]] = {
    val simplName = freshSymbol[F]("simpl")

    def step(cartoix: SMap[Symbol, Cartouche0]): F[(SMap[Symbol, Cartouche0], SMap[Symbol, CartoucheRef])] = {
      import CStage._

      /** The tuple represents the resolution of a closure: an optional
        * collapsee and the set of cartouche identifiers that collapsed.
        */
      type Resolved = List[(CStage0, Set[Symbol])]

      // Returns whether the original cartouche that was collapsed into `stage`
      // referenced identities.
      def referencesId(stage: CStage0, ref: Symbol): Boolean =
        (stage, cartoix(ref)) match {
          case (CStage.Shift(_, IncludeId, _), Cartouche.Stages(NonEmptyList(CStage.Shift(_, IdOnly, _), _))) =>
            true

          case _ =>
            false
        }

      def remapResolved(stage: CStage0, from: Symbol, to: Symbol): CartoucheRef =
        if (referencesId(stage, from))
          CartoucheRef.Offset(to, 0)
        else
          CartoucheRef.Final(to)

      val buckets = Buckets.fromCartoix(cartoix)

      for {
        simplifiedJoins <- buckets.joins.traverse(simplifyJoin[F](_))

        simplifiedBuckets = buckets.copy(joins = simplifiedJoins)

        // within subgroups, compare all possible pairs and build final subsubgroups
        // this process is O(n^3) in the size of each bucket, since computing the set
        // of disjoint reflexive transitive closures is O(n) with memoization
        joinRelation =
          Relation.allPairs(simplifiedBuckets.joins.toList)(_._1) {
            case ((_, l), (_, r)) =>
              // can't merge joins at all, so they have to be fully equal
              (l: CStage0) === r
          }

        shiftRelations =
          simplifiedBuckets.shifts mapValues { syms =>
            Relation.allPairs(syms.toList)(_._1)(κ2(true))
          }

        projectRelations =
          simplifiedBuckets.projects mapValues { syms =>
            Relation.allPairs(syms)(s => s)(κ2(true))
          }

        exprRelation =
          Relation.allPairs(simplifiedBuckets.exprs.toList)(_._1) {
            case ((_, l), (_, r)) => l.f === r.f
          }

        resolvedJoins =
          joinRelation.closures.toList map { cl =>
            // just arbitrarily pick a join; we know by construction they're all equal
            (simplifiedBuckets.joins(cl.head), cl)
          }

        resolvedProjects =
          projectRelations.toList flatMap {
            case (idx, rel) => rel.closures.toList.map((Project[T, Index](idx), _))
          }
/*
        projectedPaths =
          resolvedProjects collect {
            case (Project(p), _) => p
          }
*/
        resolvedExprs =
          exprRelation.closures.toList map { cl =>
            (simplifiedBuckets.exprs(cl.head), cl)
          }

        resolvedShifts = shiftRelations.toList flatMap {
          case (rot, rel) =>
            rel.closures.toList map { cl =>
              val shifts = cl.map(ref => ref -> simplifiedBuckets.shifts(rot)(ref)).toMap

              val (ido, eid, iid) = shifts.foldLeft((Set[Symbol](), Set[Symbol](), Set[Symbol]())) {
                case ((ido, eid, iid), (ref, Shift(_, ids, _))) =>
                  ids match {
                    case IdStatus.IdOnly => (ido + ref, eid, iid)
                    case IdStatus.IncludeId => (ido, eid, iid + ref)
                    case IdStatus.ExcludeId => (ido, eid + ref, iid)
                  }
              }

              val idStatus = if (!iid.isEmpty)
                IdStatus.IncludeId
              else if (ido.isEmpty)
                IdStatus.ExcludeId
              else if (eid.isEmpty)
                IdStatus.IdOnly
              else    // this is the case where we have both ido and eid
                IdStatus.IncludeId

              (Shift[T, Hole](Hole(), idStatus, rot), cl)
/*
              val struct0 =
                shifts.values.head.struct

              projectedPaths match {
                case h :: t =>
                  maskStruct(NonEmptyList.nels(h, t: _*), rot, struct0) match {
                    case Left(prj) => (Project[T](prj), cl)
                    case Right(struct1) => (Shift[T](struct1, idStatus, rot), cl)
                  }

                case Nil =>
                  (Shift[T](struct0, idStatus, rot), cl)
              }
*/
            }
        }

        allResolved = resolvedJoins ::: resolvedShifts ::: resolvedProjects ::: resolvedExprs

        back <- allResolved.foldLeftM((cartoix, SMap.empty[Symbol, CartoucheRef])) {
          case ((cx, remap0), (stage, syms)) =>
            val nestedCartoix =
              cx.filterKeys(syms) map {
                case (s, cart) => (s, cart.dropHead)
              }

            if (nestedCartoix forall { case (_, c) => c.isEmpty }) {
              // Pick a name to use for the cartouche
              val h = syms.head
              // Everything merged, so remap all references
              val rm1 = syms.map(fm => (fm, remapResolved(stage, fm, h))).toMap
              ((cx -- syms).updated(h, Cartouche.stages(NonEmptyList(stage))), remap0 ++ rm1).pure[F]
            } else {
              // Extract any source references of identities, removing their cartouche
              val (cx1, ids) = nestedCartoix.toList.foldLeft((SMap[Symbol, Cartouche0](), Set[Symbol]())) {
                case ((c, i), (ref, Cartouche.Source())) if referencesId(stage, ref) =>
                  (c, i + ref)

                case ((c, r), kv) => (c + kv, r)
              }

              for {
                (lower, lowerRemap) <- step(cx1)

                back <-
                  // lower coalesced into a single cartouche, increment id references and prepend to it
                  if (lower.size === 1) {
                    val (lowerName, lowerCart) = lower.head

                    val lowerRemap1 = lowerRemap map {
                      case (k, CartoucheRef.Offset(r, i)) if r === lowerName =>
                        (k, CartoucheRef.Offset(r, i + 1))

                      case other => other
                    }

                    val currentRemap = ids.map((_, CartoucheRef.Offset(lowerName, 0))).toMap

                    ((cx -- syms).updated(lowerName, stage :: lowerCart), remap0 ++ lowerRemap1 ++ currentRemap).pure[F]
                  } else {
                    // lower has multiple cartoix, turn it into a cartesian and make it the tail of a new cartouche
                    simplName map { newName =>
                      val currentRemap = ids.map((_, CartoucheRef.Offset(newName, 0))).toMap
                      val currentCart = Cartouche.stages(NonEmptyList[CStage0](stage, CStage.Cartesian(lower)))

                      ((cx -- syms).updated(newName, currentCart), remap0 ++ lowerRemap ++ currentRemap)
                    }
                  }

              } yield back
            }
        }

      } yield back
    }

    step(join.cartoix) map {
      case (simplified, remap) =>
        CStage.Join(
          simplified,
          join.joiner.map(cr => remap.getOrElse(cr.ref, cr)))
    }
  }
/*
  private def maskStruct(paths: NonEmptyList[PrjPath], rot: Rotation, struct: FreeMap)
      : Either[PrjPath, FreeMap] = {

    val pivotArr = Set(Rotation.FlattenArray, Rotation.ShiftArray)
    val pivotMap = Set(Rotation.FlattenMap, Rotation.ShiftMap)

    def isPrefix(l: PrjPath, r: PrjPath): Boolean =
      commonPrjPrefix(l, r).exists(_ === l)

    val back = for {
      structPrj <- extractProject(struct) collect {
        case FreeA(p) => p
      }

      len = structPrj.size

      prefixed = paths.list.filter(isPrefix(structPrj, _))

      mask0 = prefixed.map(_.toList.drop(len)) collect {
        case (l @ Left(_)) :: _ if pivotArr(rot) => l
        case (r @ Right(_)) :: _ if pivotMap(rot) => r
      }

      mask <- mask0.toNel
    } yield {
      if (mask.tail.isEmpty)
        Left(structPrj :::> IList(mask.head))
      else if (mask.head.isLeft)
        Right(StaticArray(mask.toList collect {
          case idx @ Left(_) => reifyPath(func.Hole, structPrj :::> IList(idx))
        }))
      else
        Right(func.StaticMapS(mask.toList collect {
          case fld @ Right(n) => (n, reifyPath(func.Hole, structPrj :::> IList(fld)))
        }: _*))
    }

    back getOrElse Right(struct)
  }
*/
  private def idsKey(ref: Symbol, offset: Int): String =
    ref.name + "_" + offset.toString

  // Eliminates Project stages, collapsing them into Shift structs or Expr nodes.
  private def finalizeStages(init: CStage.Join[T, Index, Hole]): CStage.Join[T, Nothing, FreeMap] = {
    def z(c0: CStage0): Either[NonEmptyList[CStage1], FreeMap] =
      c0 match {
        case j @ CStage.Join(_, _) => Left(NonEmptyList(finalizeStages(j): CStage1))
        case CStage.Cartesian(cx) => Left(NonEmptyList(CStage.Cartesian(finalizeCartoix(cx))))
        case CStage.Shift(_, idStatus, rot) => Left(NonEmptyList(CStage.Shift(func.Hole, idStatus, rot)))
        case CStage.Project(idx) => Right(reifyIndex(func.Hole, idx))
        case CStage.Expr(f) => Right(f)
      }

    def acc(a: NonEmptyList[CStage1] Ior FreeMap, s: CStage0): NonEmptyList[CStage1] Ior FreeMap =
      (a, s) match {
        case (Ior.Right(f), CStage.Project(idx)) => Ior.Right(reifyIndex(f, idx))
        case (Ior.Left(ss), CStage.Project(idx)) => Ior.Both(ss, reifyIndex(func.Hole, idx))
        case (Ior.Both(ss, f), CStage.Project(idx)) => Ior.Both(ss, reifyIndex(f, idx))

        case (Ior.Right(f), CStage.Shift(_, i, r)) => Ior.Left(NonEmptyList(CStage.Shift(f, i, r)))
        case (Ior.Left(ss), CStage.Shift(_, i, r)) => Ior.Left((CStage.Shift(func.Hole, i, r): CStage1) <:: ss)
        case (Ior.Both(ss, f), CStage.Shift(_, i, r)) => Ior.Left((CStage.Shift(f, i, r): CStage1) <:: ss)

        case (Ior.Right(f), CStage.Expr(g)) => Ior.Right(g >> f)
        case (Ior.Left(ss), CStage.Expr(g)) => Ior.Both(ss, g)
        case (Ior.Both(ss, f), CStage.Expr(g)) => Ior.Both(ss, g >> f)

        case (Ior.Right(f), CStage.Cartesian(cx)) =>
          Ior.Left(NonEmptyList(CStage.Cartesian(finalizeCartoix(cx)), CStage.Expr(f)))

        case (Ior.Left(ss), CStage.Cartesian(cx)) =>
          Ior.Left((CStage.Cartesian(finalizeCartoix(cx)): CStage1) <:: ss)

        case (Ior.Both(ss, f), CStage.Cartesian(cx)) =>
          Ior.Left((CStage.Cartesian(finalizeCartoix(cx)): CStage1) <:: (CStage.Expr(f): CStage1) <:: ss)

        case (Ior.Right(f), j @ CStage.Join(_, _)) =>
          Ior.Left(NonEmptyList(finalizeStages(j): CStage1, CStage.Expr(f)))

        case (Ior.Left(ss), j @ CStage.Join(_, _)) =>
          Ior.Left((finalizeStages(j): CStage1) <:: ss)

        case (Ior.Both(ss, f), j @ CStage.Join(_, _)) =>
          Ior.Left((finalizeStages(j): CStage1) <:: (CStage.Expr(f): CStage1) <:: ss)
      }

    def finalizeCartouche(c0: Cartouche0): Cartouche1 =
      c0 match {
        case Cartouche.Stages(ss0) =>
          Foldable1[NonEmptyList].foldMapLeft1(ss0)(s => Ior.fromEither(z(s)))(acc) match {
            case Ior.Left(ss1) => Cartouche.stages[T, Nothing, FreeMap](ss1.reverse)
            case Ior.Right(fm) => Cartouche.stages[T, Nothing, FreeMap](NonEmptyList(CStage.Expr(fm): CStage1))
            case Ior.Both(ss1, fm) => Cartouche.stages[T, Nothing, FreeMap]((CStage.Expr(fm) <:: ss1).reverse)
          }

        case Cartouche.Source() => Cartouche.source
      }

    def finalizeCartoix(cx: SMap[Symbol, Cartouche0]): SMap[Symbol, Cartouche1] =
      cx map { case (k, v) => (k, finalizeCartouche(v)) }

    CStage.Join(finalizeCartoix(init.cartoix), init.joiner)
  }

  private def reifyJoin[
      G[_]: Monad: MonadPlannerErr: NameGenerator: RevIdxM: MinStateM[T, P, ?[_]]](
      join: CStage.Join[T, Nothing, FreeMap],
      parent: QSUGraph,
      lens: StructLens,
      isNested: Boolean)
      : G[QSUGraph] = {

    val (srcRefs, rest) = join.cartoix.toList.foldRight((Set[Symbol](), List[(Symbol, NonEmptyList[CStage1])]())) {
      case ((s, Cartouche.Source()), (as, r)) => (as + s, r)
      case ((s, Cartouche.Stages(ss)), (as, r)) => (as, (s, ss) :: r)
    }

    for {
      results <- reifyCartoix[G](parent, rest, lens, isNested)

      f = join.joiner flatMap {
        case CartoucheRef.Final(s) if srcRefs(s) =>
          func.ProjectKeyS(func.Hole, SourceKey)

        case CartoucheRef.Final(s) =>
          func.ProjectKeyS(func.Hole, s.name)

        case CartoucheRef.Offset(s, off) =>
          func.ProjectKeyS(func.Hole, idsKey(s, off))
      }

      mapped <- results match {
        case LeftShift(src, struct, idStatus, onUndef, repair, rot) =>
          val joinRep = MapFuncCore.normalized(f >> repair)
          updateGraph[G](src)(QSU.LeftShift(_, struct, idStatus, onUndef, joinRep, rot))

        case other =>
          updateGraph[G](other)(QSU.Map(_, f.asRec))
      }
    } yield mapped
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def reifyCartoix[
      G[_]: Monad: MonadPlannerErr: NameGenerator: RevIdxM: MinStateM[T, P, ?[_]]](
      parent: QSUGraph,
      cartoix: List[(Symbol, NonEmptyList[CStage1])],
      lens0: StructLens,
      isNested: Boolean)
      : G[QSUGraph] =
    cartoix.sortBy(kv => reifyPrecedence(kv._2.head)) match {
      case (s, NonEmptyList(hd, tail)) :: rest =>
        val prj =
          if (isNested)
            lens0.project
          else
            func.ProjectKeyS(func.Hole, SourceKey)

        val inj =
          new ∀[λ[α => (Symbol, Int, FreeMapA[α], FreeMapA[α], Option[FreeMapA[α]]) => FreeMapA[α]]] {
            def apply[α] = { (id, offset, results, above, maybeIds) =>
              val core = func.ConcatMaps(
                above,
                func.MakeMapS(id.name, results))

              maybeIds.fold(core) { ids =>
                func.ConcatMaps(
                  core,
                  func.MakeMapS(idsKey(id, offset), ids))
              }
            }
          }

        val resultsM = reifyStage[G](s, 0, hd, parent, lens0, true) flatMap { parent =>
          val projectPrev = func.ProjectKeyS(func.Hole, s.name)

          tail.zipWithIndex.foldLeftM(parent) {
            case (parent, (stage, i)) =>
              reifyStage[G](s, i + 1, stage, parent, StructLens(projectPrev, inj, true), true)
          }
        }

        resultsM.flatMap(reifyCartoix[G](_, rest, StructLens(prj, inj, true), isNested))

      case Nil =>
        parent.pure[G]
    }

  private def reifyIndex[A](src: FreeMapA[A], idx: Index): FreeMapA[A] =
    idx.toEither.fold(func.ProjectIndexI(src, _), func.ProjectKeyS(src, _))

  private def reifyPath[A, F[_]: Foldable](z: FreeMapA[A], path: F[Index])
      : FreeMapA[A] =
    path.foldLeft(z)(reifyIndex)

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def reifyStage[
      G[_]: Monad: MonadPlannerErr: NameGenerator: RevIdxM: MinStateM[T, P, ?[_]]](
      id: Symbol,
      offset: Int,
      stage: CStage1,
      parent: QSUGraph,
      lens: StructLens,
      isNested: Boolean)
      : G[QSUGraph] = {

    stage match {
      case CStage.Expr(f) =>
        val ap = λ[FreeMapA ~> FreeMapA](f >> lens.project >> _)

        parent match {
          case LeftShift(src, struct, idStatus, onUndef, repair, rot) =>
            updateGraph[G](src)(
              QSU.LeftShift(
                _,
                struct,
                idStatus,
                onUndef,
                lens.inject[JoinSide](
                  id,
                  offset,
                  MapFuncCore.normalized(ap(repair)),
                  repair,
                  None),
                rot))

          case Map(src, rfm) =>
            updateGraph[G](src)(
              QSU.Map(
                _,
                lens.inject[Hole](
                  id,
                  offset,
                  MapFuncCore.normalized(ap(rfm.linearize)),
                  func.Hole,
                  None).asRec))

          case src =>
            updateGraph[G](src)(
              QSU.Map(
                _,
                lens.inject[Hole](
                  id,
                  offset,
                  MapFuncCore.normalized(ap(func.Hole)),
                  func.Hole,
                  None).asRec))
        }

      case CStage.Shift(struct, idStatus, rot) =>
        updateGraph[G](parent)(
          QSU.LeftShift(
            _,
            (struct >> lens.project).asRec,
            idStatus,
            if (lens.outer) OnUndefined.Emit else OnUndefined.Omit,
            lens.inject[JoinSide](
              id,
              offset,
              idStatus match {
                case IncludeId => func.ProjectIndexI(func.RightSide, 1)
                case _ => func.RightSide
              },
              func.LeftSide,
              idStatus match {
                case IncludeId => Some(func.ProjectIndexI(func.RightSide, 0))
                case _ => None
              }),
            rot))

      case CStage.Cartesian(cartoix) =>
        val cartoix2 = cartoix.toList map {
          case (k, Cartouche.Stages(ss)) => (k, ss)
          case (k, Cartouche.Source()) => (k, NonEmptyList[CStage1](CStage.Expr(func.Hole)))
        }

        reifyCartoix[G](parent, cartoix2, lens, isNested)

      case j @ CStage.Join(_, _) =>
        reifyJoin[G](j, parent, lens, isNested)

      case CStage.Project(_) =>
        absurd
    }
  }

  // Order sibling cartouches when rendering to minimize
  // structure and provide stability for tests
  private val reifyPrecedence: CStage1 => Int = {
    case CStage.Join(_, _) => 0
    case CStage.Cartesian(_) => 1
    case CStage.Shift(_, _, _) => 2
    case CStage.Expr(_) => 3
    case CStage.Project(_) => absurd
  }

  private def updateGraph[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      parent: QSUGraph)(
      nodeF: Symbol => QSU[T, Symbol])
      : G[QSUGraph] =
    for {
      g <- derive[G](parent)(nodeF)
      _ <- MinimizeAutoJoins.updateProvenance[T, G](qprov, g)
    } yield g

  private def derive[
      G[_]: Monad: NameGenerator: RevIdxM](
      parent: QSUGraph)(
      nodeF: Symbol => QSU[T, Symbol])
      : G[QSUGraph] =
    withName[G](nodeF(parent.root)).map(_ :++ parent)

  private def withName[
      G[_]: Monad: NameGenerator: RevIdxM](
      node: QSU[T, Symbol])
      : G[QSUGraph] =
    QSUGraph.withName[T, G]("mcart")(node)

  // this isn't in the stdlib wtf?
  implicit class Function2Syntax[A, B, C](self: (A, B) => C) {
    def andThen[D](f: C => D): (A, B) => D =
      (a, b) => f(self(a, b))
  }

  private case class StructLens(
      project: FreeMap,
      // (cartouche id, offset, result access, incoming value, identity access)
      inject: ∀[λ[α => (Symbol, Int, FreeMapA[α], FreeMapA[α], Option[FreeMapA[α]]) => FreeMapA[α]]],
      outer: Boolean)

  private object StructLens {
    def init(includeSource: Boolean): StructLens = {
      val inj =
        new ∀[λ[α => (Symbol, Int, FreeMapA[α], FreeMapA[α], Option[FreeMapA[α]]) => FreeMapA[α]]] {
          def apply[α] = { (id, offset, results, above, maybeIds) =>
            val core =
              if (includeSource)
                func.StaticMapS(
                  SourceKey -> above,
                  id.name -> results)
              else
                func.MakeMapS(id.name, results)

            maybeIds.fold(core) { ids =>
              func.ConcatMaps(
                core,
                func.MakeMapS(idsKey(id, offset), ids))
            }
          }
        }

      StructLens(func.Hole, inj, true)
    }
  }
}

object MergeCartoix {

  def apply[T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT](
      qp: QProv[T])(
      implicit eqP: Eq[qp.P])
      : Minimizer.Aux[T, qp.P] =
    new MergeCartoix[T] {
      val qprov: qp.type = qp
      val PEqual = eqP
    }
}
