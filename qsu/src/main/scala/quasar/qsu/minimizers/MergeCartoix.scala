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

package quasar.qsu
package minimizers

import slamdata.Predef._

import matryoshka.{BirecursiveT, EqualT, ShowT}

import quasar.common.CPathNode
import quasar.common.effect.NameGenerator
import quasar.qscript.{construction, MonadPlannerErr, RecFreeS}
import quasar.qsu.{QScriptUniform => QSU}

import scalaz.{Monad, Scalaz}, Scalaz._

import scala.collection
import scala.collection.{Map => SMap}

final class MergeCartoix[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends Minimizer[T] {
  import MinimizeAutoJoins.MinStateM
  import QSUGraph.withName
  import QSUGraph.Extractors._
  import RecFreeS.RecOps

  private val AboveKey = "above"

  private val func = construction.Func[T]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean =
    candidates exists { case Transpose(_, _, _) => true; case _ => false }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def extract[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, ?[_]]](
      candidate: QSUGraph)
      : Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = {

    candidate match {
      case qgraph @ Transpose(parent, retain, rotations) =>
        extract[G](parent) map {
          _ rightMap {
            _ andThen {
              _ flatMap { parent2 =>
                derive[G](parent2)(QSU.LeftShift(_, retain, rotations))
              }
            }
          }
        }

      case MappableRegion.MaximalUnary(Transpose(parent, retain, rotations), fm) =>
        extract[G](parent) map {
          _ rightMap {
            _ andThen {
              _ flatMap { parent2 =>
                derive[G](parent2)(QSU.LeftShift(_, retain, rotations))
              }
            }
          }
        }

      case MappableRegion.MaximalUnary(parent, fm) =>
        Some((
          parent,
          (parent2, fm2) => derive[G](parent2)(QSU.Map(_, (fm >> fm2).asRec))))
    }
  }

  // the first component of the tuple is the rewrite target on any provenance association
  // the second component is the root of the resulting graph
  def apply[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, ?[_]]](
      original: QSUGraph,
      singleSource: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int])
      : G[Option[(QSUGraph, QSUGraph)]] = {

    val optJoin: Option[CStage.Join[T]] =
      candidates.traverse(readCandidate) map { cartoix =>
        val mapped: SMap[Symbol, Cartouche[T]] = cartoix.zipWithIndex.map({
          case (c, i) => Symbol(s"cart$i") -> c
        })(collection.breakOut)

        CStage.Join[T](func.Hole, mapped, fm.map(i => CartoucheRef.Final(Symbol(s"cart$i"))))
      }

    optJoin.map(simpifyJoin).traverse(reifyStage[G](_)).map(_.map(g => (g, g)))
  }

  // returns the list of stages in reverse order
  // ignores projection
  private def readCandidates(
      dims: SMap[Symbol, QDims[T]],
      singleSource: QSUGraph,
      qgraph: QSUGraph)
      : List[CStage] = qgraph match {

    case Transpose(src, retain, rot) =>
      val above = MappableRegion[T](singleSource.root ===, src)
      val cartoix = above.map(g => (g.root, readCandidate(singleSource, g)))
      val realCount = cartoix.foldMap(_._2.length)

      if (realCount > 1) {
        val cmap = cartoix.foldMap(pair => Map(pair)) map {
          case (root, stages) =>
            Cartouche(stages.reverse, dims(root))
        }

        val joiner = cartoix.map(_._1).map(CartoucheRef.Final)
        val join = CStage.Join(cmap, joiner)
        val current = CStage.Shift(func.Hole, retain.fold(IdOnly, ExcludeId), rot)

        // this is an interesting discovery: joins are fully subsuming during read
        // it's justified because we haven't (yet) collapsed above, so there can be
        // no common parents yet. in other words, we read as a tree and collapse to
        // a dag
        current :: join :: Nil
      } else {
        val incoming = cartoix.as(Hole())
        val parent = cartoix.toList.flatMap(_._2)
        val current = CStage.Shift(incoming, retain.fold(IdOnly, ExcludeId), rot)

        current :: parent
      }

    case qgraph =>
      assert(qgraph.root === singleSource.root)
      Nil
  }

  /*private def factorProjections(stage: CStage): List[CStage] = ???

  private def extractProjections[A](
      fm: FreeMapA[A])
      : FreeMapA[A \/ (CPathNode, FreeMapA[A])] = {

    /*
     * - extractProjectionsƒ has the type FreeMapA[A] => FreeMapA[A \/ (CPathNode, FreeMapA[A])] \/ CoEnv[MapFunc, FreeMapA[A]]
     * - Stick it into elgotApo with fm as the A
     * - With each stage, manually call .project on
     *   the freemap
     * - If you find a projection, make the cpath node,
     *   pair it up with the other side of .project and
     *   point it into FreeMapA
     * - If you find an A, make a left, and point it
     * - If it's something else, throw it into CoEnv
     *   and return it on the right
     */

    interpret(fm, ...).project
  }*/

  private def simpifyJoin(join: CStage.Join[T]): Either[Cartouche, CStage.Join[T]] = {
    case class Buckets(
        joins: Set[Symbol],
        shifts: SMap[Rotation, Set[Symbol]],
        projects: SMap[CPathNode, Symbol])

    object Buckets {
      val Empty = Buckets(Set(), SMap(), SMap())

      // get the sets from above and bucket all the cartoix referenced
      // everything else goes in a separate bucket
      // within each bucket, subgroup by compatibility potential
      //   1. Node type
      //   2. Project ref
      //   3. Shift rotation
      // note that subgrouping doesn't mean guaranteed compatible
      def fromStages(incomplete: SMap[Symbol, Cartouche[T]]): Buckets = {
        import CStage._

        incomplete.foldLeft(Empty) {
          case (buckets, (ref, Cartouche(head :: _, _))) =>
            import CStage._

            head match {
              case Join(_, _, _) =>
                buckets.copy(joins = buckets.joins + ref)

              case Shift(_, _, rot) =>
                val shiftsSet = buckets.shifts.getOrElse(rot, Set()) + ref
                buckets.copy(shifts = buckets.shifts + (rot -> shiftsSet))

              case Project(_, n) =>
                buckets.copy(projects = buckets.projects + (n -> ref))
            }

          case (_, (_, Cartouche(Nil, _))) =>
            sys.error("empty cartouche spotted (impossible)")
        }
      }
    }

    case class Residual(stages: List[CStage], rewrites: Map[Symbol, Option[Int]])

    def step(
        incomplete: Map[Symbol, Cartouche],
        above: List[CStage],
        rewrites: Map[Symbol, Option[Int]]): Residual = {

      import CStage._
      import state._

      val buckets = Buckets.fromStages(incomplete)

      // within subgroups, compare all possible pairs and build final subsubgroups
      // this process is O(n^3) in the size of each bucket, since computing the set
      // of disjoint reflexive transitive closures is O(n) with memoization
      val joinRelation = Relation.allPairs(buckets.joins) { (js1, js2) =>
        // can't merge joins at all, so they have to be fully equal
        incomplete(js1) === incomplete(js2)
      }

      val shiftRelations = buckets.shifts mapValues { syms =>
        Relation.allPairs(syms) { (ss1, ss2) =>
          incomplete(ss1).incoming === incomplete(ss2).incoming
        }
      }

      val projectRelations = buckets.projects mapValues { syms =>
        Relation.allPairs(syms) { (ps1, ps2) =>
          incomplete(ps1).incoming === incomplete(ps2).incoming
        }
      }

      /*
       * The tuple represents the resolution of a closure: an optional
       * collapsee and the set of cartouche identifiers which "went into"
       * it. The second component of the outer tuple is the set of
       * identifiers that were "left over", and couldn't be merged.
       * This set of leftovers is only relevant when the computation of
       * the reflexive transitive closure is not sufficient to fully
       * determine mergeability. In other words, it applies when the
       * mapping process could determine that related stages are not in
       * fact mergeable (i.e. making a decision).
       */
      type States = Set[(Option[(CStage, Set[Symbo])], Set[Symbol])]

      // collapse final subsubgroups

      val joinStates: States = joinRelation.closures map { cl =>
        // just arbitrarily pick a join; we know by construction they're all equal
        (Some((incomplete(cl.head).head, cl)), Set())
      }

      val shiftStates: States = shiftRelations.values flatMap { rel =>
        rel.closures flatMap { cl =>
          val shifts = cl flatMap { ref =>
            incomplete.get(ref) collect {
              case s @ Shift(_, _, _) => ref -> s
            }
          } toMap

          val (ido, eid, iid) = shifts.foldLeft((Set[Symbol](), Set[Symbol](), Set[Symbol]())) {
            case ((ido, eid, iid), (ref, Shift(_, ids, _))) =>
              ids match {
                case IdStatus.IdOnly => (ido + ref, eid, iid)
                case IdStatus.IncludeId => (ido, eid, iid + ref)
                case IdStatus.ExcludeId => (ido, eid + ref, iid)
              }
          }

          val (_, Shift(incoming, _, rot)) = shifts.head

          val idStatus = if (!iid.isEmpty)
            IdStatus.IncludeId
          else if (ido.isEmpty)
            IdStatus.ExcludeId
          else if (eid.isEmpty)
            IdStatus.IdOnly
          else    // this is the case where we have both ido and eid
            IdStatus.IncludeId

          // these are the IdOnly cartoix which will go to Undefined because they're wrong
          // we don't have an undefined expression here though, so we just don't collapse
          val undefineds = ido.filter(ref => incomplete(ref).stages.nonEmpty)
          val ido2 = ido - undefineds

          val reduced = incomplete.filterKeys(eid ++ iid) flatMap {
            case (ref, Cartouche(hd :: Nil, dims)) =>
              None

            case (ref, Cartouche(hd :: tail, dims)) =>
              Some(ref -> Cartouche(tail, dims))
          }

          val collapsed = State.init(
            incomplete.filterKeys(reduced),
            Shift(incoming, idStatus, rot))

          val uncollapsed = undefineds map { ref =>
            State.init(SMap(ref -> incomplete(ref)), )
          }

          uncollapsed ++ collapsed
        }
      }

      val projectStates: Set[State] = projectRelations.values flatMap { rel =>
        rel.closures flatMap { cl =>
          val projects = cl flatMap { ref =>
            incomplete.get(ref) collect {
              case p @ Project(_, ) => ref -> p
            }
          } toMap


        }
      }

      val states = joinStates ++ shiftStates ++ projectStates

      val residuals = if (states.size === 1) {
        states.head match {
          case (Some((collapsed, from)), remainder) =>


          case (None, remainder) =>
            // ok the problem here is `above`!
            // the Join representation conflates common prefix
            // with resolvable common suffix (which doesn't exist)
            // we probably need to add a CStage.Residual or something
            remainder.map(step)
        }
      } else {

      }
    }

    step(State.fromJoin(join)).toJoin
  }

  private def eliminateSingletonJoins(join: CStage.Join[T]): CStage.Join[T] =
    join    // TODO traverse up the hierarchy and eliminate joins with a single cartouche

  private def reifyStage[
      G[_]: Monad: NameGenerator: RevIdxM](
      stage: CStage[T],
      parent: QSUGraph,
      lens: StructLens)
      : G[QSUGraph] = {

    import lens._

    def idsKey(ref: Symbol, offset: Int): CPathField =
      CPathField(ref.name + "_" + offset.toString)

    def reifyFirstCartouche(
        parent: QSUGraph,
        cartoix: List[(Symbol, Cartouche[T])])
        : G[QSUGraph] = cartoix match {

      case (s @ Symbol(cname), Cartouche(stages, _)) :: rest =>
        stages match {
          case hd :: tail =>
            // we can ignore parent inject for now since we're preserving the "above"
            def inject2(offset: Int) =
              new ∀[λ[α => (FreeMapA[α], FreeMapA[α], Option[FreeMapA[α]]) => FreeMapA[α]]] {
                def apply[α] = { (results, above, maybeIds) =>
                  val core = func.StaticMapS(
                    cname -> results,
                    AboveKey -> above)

                  maybeIds map { ids =>
                    func.ConcatMaps(
                      core,
                      func.StaticMapS(idsKey(s, offset).value -> ids))
                  } getOrElse core
                }
              }

            val lens2 = StructLens(project, inject2(0), true)
            val resultsM = reifyStage[G](hd, parent, lens2) flatMap { parent =>
              val project2 = func.ProjectKeyS(func.Hole, cname)

              tail.zipWithIndex.foldLeftM(parent) {
                case (parent, (stage, i)) =>
                  reifyStage[G](stage, parent, StructLens(project2, inject2(i + 1), true))
              }
            }

            resultsM.flatMap(reifyMiddleCartoix(CPathField(cname), _, rest))

          case Nil =>
            sys.error("empty cartouche should be impossible")
        }

      case Nil =>
        sys.error("empty join should be impossible")
    }

    // TODO manage dims
    def reifyMiddleCartoix(
        previous: CPathField,
        parent: QSUGraph,
        cartoix: List[(Symbol, Cartouche[T])])
        : G[QSUGraph] = cartoix match {

      case (Symbol(cname), Cartouche(stages, _)) :: rest =>
        val project2 = func.ProjectKeyS(func.Hole, previous.value)

        def inject2(offset: Int) =
          new ∀[λ[α => (FreeMapA[α], FreeMapA[α], Option[FreeMapA[α]]) => FreeMapA[α]]] {
            def apply[α] = { (results, above, maybeIds) =>
              val core = func.ConcatMaps(
                above,
                func.StaticMapS(cname -> results))

              maybeIds map { ids =>
                func.ConcatMaps(
                  core,
                  func.StaticMapS(idsKey(s, offset).value -> ids))
              } getOrElse core
            }
          }

        stages match {
          case hd :: tail =>
            val lens2 = StructLens(project2, inject2(0), true)
            val resultsM = reifyStage[G](hd, parent, lens2) flatMap { parent =>
              val project2 = func.ProjectKeyS(func.Hole, cname)

              tail.zipWithIndex.foldLeftM(parent) {
                case (parent, (stage, i)) =>
                  reifyStage[G](stage, parent, StructLens(project2, inject2(i + 1), true))
              }
            }

            resultsM.flatMap(reifyMiddleCartoix(CPathField(cname), _, rest))

          case Nil =>
            sys.error("empty cartouche should be impossible")
        }

      case Nil =>
        parent.point[G]
    }

    stage match {
      case CStage.Project(incoming, CPathField(field)) =>
        derive[G](parent)(
          QSU.Map(
            _,
            inject[Hole](func.ProjectKeyS(incoming >> project, field), func.Hole, None)))

      case CStage.Project(incoming, CPathIndex(idx)) =>
        derive[G](parent)(
          QSU.Map(
            _,
            inject[Hole](func.ProjectIndexI(incoming >> project, idx), func.Hole, None)))

      case CStage.Shift(incoming, idStatus, rot) =>
        derive[G](parent)(
          QSU.LeftShift(
            _,
            incoming >> project,
            idStatus,
            if (outer) OnUndefined.Emit else OnUnefined.Omit,
            inject[JoinSide](
              idStatus match {
                case IncludeId => func.ProjectIndexI(func.RightSide, 1)
                case _ => func.RightSide
              },
              func.LeftSide,
              idStatus match {
                case IncludeId => Some(func.ProjectIndexI(func.RightSide, 0))
                case _ => None
              })))

      // safe to assume cartoix is non-empty
      case CStage.Join(incoming, cartoix, joiner) =>
        for {
          wrappedParent <- if (incoming === func.Hole)
            parent.point[G]
          else
            derive[G](parent)(QSU.Map(_, inject[Hole](incoming >> project, func.Hole, None)))

          results <- reifyFirstCartouche(wrappedParent, cartoix)

          mapped <- derive[G](results)(
            QSU.Map(
              _,
              inject[Hole](
                joiner flatMap {
                  case CartoucheRef.Final(s) =>
                    func.ProjectKeyS(func.Hole, s.name)

                  case CartoucheRef.Offset(s, off) =>
                    func.ProjectKeyS(func.Hole, idsKey(ref, off).value)
                },
                func.ProjectKeyS(func.Hole, AboveKey),
                None)))
        } yield mapped
    }
  }

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
      inject: ∀[λ[α => (FreeMapA[α], FreeMap[α], Option[FreeMapA[α]]) => FreeMapA[α]]],
      outer: Boolean)
}

object MergeCartoix {

  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT]: MergeCartoix[T] =
    new MergeCartoix[T]
}
