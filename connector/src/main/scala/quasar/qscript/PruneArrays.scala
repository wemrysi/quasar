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

package quasar.qscript

import quasar.Predef.{ Map => ScalaMap, _ }
import quasar.fp.ski._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import simulacrum.typeclass

object PATypes {
  type SeenIndices = Set[BigInt]
  type KnownIndices = Option[SeenIndices]

  implicit final class KnownIndicesOps(val self: KnownIndices) extends AnyVal {
    /** The standard Semigroup on Option chooses one value if both are `Some`.
      * This appends them.
      */
    def |++|(other: KnownIndices): KnownIndices =
      Semigroup.liftSemigroup[Option, SeenIndices].append(self, other)
  }
}

@typeclass trait PruneArrays[F[_]] {
  import PATypes._

  def find[M[_], A](in: F[A])(implicit M: MonadState[M, KnownIndices]): M[KnownIndices]
  def remap[M[_], A](env: KnownIndices, in: F[A])(implicit M: MonadState[M, KnownIndices])
      : M[F[A]]
}

class PAHelpers[T[_[_]]: BirecursiveT: EqualT] extends TTypes[T] {
  import PATypes._

  type IndexMapping = ScalaMap[BigInt, BigInt]

  def mergeMaps[A](a: ScalaMap[A, KnownIndices], b: ScalaMap[A, KnownIndices]) =
    a.alignWith(b) {
      case \&/.This(k)      => k
      case \&/.That(k)      => k
      case \&/.Both(k1, k2) => k1 |++| k2
    }

  /** Returns `None` if a non-static non-integer index was found.
    * Else returns all indices of the form `ProjectIndex(SrcHole, IntLit(_))`.
    */
  def findIndicesInFunc[A](func: Free[MapFunc, A]): ScalaMap[A, KnownIndices] =
    func.project.run.fold(k => ScalaMap(k -> none), κ(findIndicesInStruct(func)))

  def findIndicesInStruct[A](func: Free[MapFunc, A]): ScalaMap[A, KnownIndices] = {
    def accumulateIndices: GAlgebra[(Free[MapFunc, A], ?), MapFunc, ScalaMap[A, KnownIndices]] = {
      case ProjectIndex((src, acc1), (value, acc2)) =>
        val newMap = mergeMaps(acc1, acc2)
          (src.project.run, value.project.run) match {
          case (-\/(k), \/-(IntLitMapFunc(idx))) => newMap + (k -> newMap.get(k).fold(Set(idx).some)(_.map(_ + idx))) // static integer index
          case (-\/(k), _)                       => newMap + (k -> none) // non-static index
          case (_,      _)                       => newMap
        }
      // check if entire array is referenced
      case f => f.foldRight(ScalaMap.empty[A, KnownIndices])((elem, acc) => elem match {
        case (Embed(CoEnv(-\/(k))), value) => mergeMaps(value, acc) + (k -> none)
        case (_,                    value) => mergeMaps(value, acc)
      })
    }

    def findIndices: GAlgebra[(Free[MapFunc, A], ?), CoEnv[A, MapFunc, ?], ScalaMap[A, KnownIndices]] =
      _.run.fold(k => ScalaMap.empty, accumulateIndices)

    func.para(findIndices)
  }

  def remapResult[A](hole: FreeMapA[A], mapping: IndexMapping, idx: BigInt): CoEnv[A, MapFunc, FreeMapA[A]] =
    CoEnv[A, MapFunc, FreeMapA[A]](\/-(ProjectIndex(hole, IntLit(mapping.get(idx).getOrElse(idx)))))

  /** Remap all indices in `func` in structures like
    * `ProjectIndex(SrcHole, IntLit(_))` according to the provided `mapping`.
    */
  def remapIndicesInFunc(func: FreeMap, mapping: IndexMapping): FreeMap =
    func.transCata[FreeMap] {
      case CoEnv(\/-(ProjectIndex(hole @ Embed(CoEnv(-\/(SrcHole))), IntLit(idx)))) =>
        remapResult[Hole](hole, mapping, idx)
      case co => co
    }

  def remapIndicesInLeftShift[A](struct: FreeMap, repair: JoinFunc, mapping: IndexMapping): JoinFunc =
    repair.transCata[JoinFunc] {
      case CoEnv(\/-(ProjectIndex(hole @ Embed(CoEnv(-\/(LeftSide))), IntLit(idx)))) =>
        remapResult[JoinSide](hole, mapping, idx)
      case CoEnv(\/-(ProjectIndex(hole @ Embed(CoEnv(-\/(RightSide))), IntLit(idx)))) if struct ≟ HoleF =>
        remapResult[JoinSide](hole, mapping, idx)
      case co => co
    }

  /** Prune the provided `array` keeping only the indices in `indicesToKeep`. */
  def arrayRewrite(array: ConcatArrays[T, JoinFunc], indicesToKeep: Set[Int]): JoinFunc = {
    val rewrite = new Rewrite[T]

    def removeUnusedIndices[A](array: List[A], indicesToKeep: Set[Int]): List[A] =
      indicesToKeep.toList.sorted map array

    rewrite.rebuildArray[JoinSide](
      removeUnusedIndices[JoinFunc](rewrite.flattenArray[JoinSide](array), indicesToKeep))
  }

  // TODO: Can we be more efficient? - can get rid of `.sorted`, but might be
  //       non-deterministic, then.
  val indexMapping: SeenIndices => IndexMapping =
    _.toList.sorted.zipWithIndex.map(_.rightMap(BigInt(_))).toMap
}

// TODO `find` and `remap` impls should be returning a free algebra
// which is interpreted separately
object PruneArrays {
  import PATypes._

  private def haltRemap[M[_], A](out: A)(implicit M: MonadState[M, KnownIndices])
      : M[A] =
    M.put(none).as(out)

  private def default[IN[_]]
      : PruneArrays[IN] =
    new PruneArrays[IN] {
      def find[M[_], A](in: IN[A])(implicit M: MonadState[M, KnownIndices]) =
        M.put(none).as(none)
      def remap[M[_], A](env: KnownIndices, in: IN[A])(implicit M: MonadState[M, KnownIndices]) =
        haltRemap(in)
    }

  implicit def coproduct[I[_], J[_]]
    (implicit I: PruneArrays[I], J: PruneArrays[J])
      : PruneArrays[Coproduct[I, J, ?]] =
    new PruneArrays[Coproduct[I, J, ?]] {

      def find[M[_], A](in: Coproduct[I, J, A])(implicit M: MonadState[M, KnownIndices]) =
        in.run.fold(I.find[M, A], J.find[M, A])

      def remap[M[_], A](env: KnownIndices, in: Coproduct[I, J, A])(implicit M: MonadState[M, KnownIndices]) =
        in.run.fold(
          I.remap(env, _) ∘ Coproduct.leftc,
          J.remap(env, _) ∘ Coproduct.rightc)
    }

  implicit def read[A]: PruneArrays[Const[Read[A], ?]] = default
  implicit def shiftedRead[A]: PruneArrays[Const[ShiftedRead[A], ?]] = default
  implicit def deadEnd: PruneArrays[Const[DeadEnd, ?]] = default

  // TODO examine branches
  implicit def thetaJoin[T[_[_]]]: PruneArrays[ThetaJoin[T, ?]] = default
  // TODO examine branches
  implicit def equiJoin[T[_[_]]]: PruneArrays[EquiJoin[T, ?]] = default

  implicit def projectBucket[T[_[_]]: BirecursiveT: EqualT]
      : PruneArrays[ProjectBucket[T, ?]] =
    new PruneArrays[ProjectBucket[T, ?]] {

      val helpers = new PAHelpers[T]
      import helpers._

      private def findInBucket[M[_]](fm1: FreeMap, fm2: FreeMap)(implicit M: MonadState[M, KnownIndices])
          : M[KnownIndices] =
        M.put(extractFromMap(findIndicesInFunc(fm1), SrcHole) |++| extractFromMap(findIndicesInFunc(fm2), SrcHole)).as(none)

      def find[M[_], A](in: ProjectBucket[A])(implicit M: MonadState[M, KnownIndices]) =
        in match {
          case BucketField(_, value, name) => findInBucket(value, name)
          case BucketIndex(_, value, index) => findInBucket(value, index)
        }

      def remap[M [_], A](env: KnownIndices, in: ProjectBucket[A])(implicit M: MonadState[M, KnownIndices]) =
        M.get >>= (st => haltRemap(st.fold(in)(indexMapping >>> (repl => in match {
          case BucketField(src, value, name) =>
            BucketField(src, remapIndicesInFunc(value, repl), remapIndicesInFunc(name, repl))
          case BucketIndex(src, value, index) =>
            BucketIndex(src, remapIndicesInFunc(value, repl), remapIndicesInFunc(index, repl))
        }))))
    }

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT]
      : PruneArrays[QScriptCore[T, ?]] =
    new PruneArrays[QScriptCore[T, ?]] {

      val helpers = new PAHelpers[T]
      import helpers._

      def find[M[_], A](in: QScriptCore[A])(implicit M: MonadState[M, KnownIndices]) =
        in match {
          case LeftShift(_, struct, _, repair) =>
            val newState =
              extractFromMap(findIndicesInFunc(repair), LeftSide) |++| extractFromMap(findIndicesInFunc(struct), SrcHole)

            M.get >>= (st => M.put(newState).as(repair.resume match {
              case -\/(ConcatArrays(_, _)) => st // annotate state as environment
              case _                       => none
            }))

          case Reduce(src, bucket, reducers, _) =>
            val bucketState: KnownIndices = extractFromMap(findIndicesInFunc(bucket), SrcHole)
            val reducersState: KnownIndices = reducers.foldMap(_.foldMap[KnownIndices](r => extractFromMap(findIndicesInFunc(r), SrcHole)))
            M.put(bucketState |++| reducersState).as(none)

          case Union(_, _, _)     => default.find(in) // TODO examine branches
          case Subset(_, _, _, _) => default.find(in) // TODO examine branches

          case Map(_, func)    => M.put(extractFromMap(findIndicesInFunc(func), SrcHole)).as(none)
          case Filter(_, func) => M.modify(extractFromMap(findIndicesInFunc(func), SrcHole) |++| _).as(none)

          case Sort(_, bucket, order) =>
            val bucketState: KnownIndices = extractFromMap(findIndicesInFunc(bucket), SrcHole)
            val orderState: KnownIndices = order.foldMap {
              case (f, _) => extractFromMap(findIndicesInFunc(f), SrcHole)
            }
            M.modify(bucketState |++| orderState |++| _).as(none)

          case Unreferenced() => default.find(in)
        }

      def remap[M[_], A](env: KnownIndices, in: QScriptCore[A])(implicit M: MonadState[M, KnownIndices]) =
        // ignore `env` everywhere except for `LeftShift`
        in match {
          case LeftShift(src, struct, id, repair) =>
            def rewriteRepair(array: ConcatArrays[T, JoinFunc], acc: SeenIndices): JoinFunc  =
              arrayRewrite(array, acc.map(_.toInt).toSet)

            def replacementRemap(repl: IndexMapping): QScriptCore[A] =
              LeftShift(src,
                remapIndicesInFunc(struct, repl),
                id,
                remapIndicesInLeftShift(struct, repair, repl))

            repair.resume match {
              case -\/(array @ ConcatArrays(_, _)) =>
                env.cata(
                  acc => {
                    def replacement(repl: IndexMapping): QScriptCore[A] =
                      LeftShift(src,
                        remapIndicesInFunc(struct, repl),
                        id,
                        remapIndicesInLeftShift(struct, rewriteRepair(array, acc), repl))
                    M.put(env).as(
                      env.fold[QScriptCore[A]](
                        LeftShift(src, struct, id, rewriteRepair(array, acc)))(
                        indexMapping >>> replacement))
                  },
                  M.get >>= (st => haltRemap(st.fold(in)(indexMapping >>> replacementRemap))))
              case _ =>
                M.get >>= (st => haltRemap(st.fold(in)(indexMapping >>> replacementRemap)))
            }

          case Reduce(src, bucket0, reducers0, repair) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Reduce(
                src,
                remapIndicesInFunc(bucket0, repl),
                reducers0.map(_.map(remapIndicesInFunc(_, repl))),
                repair)
            M.get >>= (st => haltRemap(st.fold(in)(indexMapping >>> replacement)))

          case Union(_, _, _)     => default.remap(env, in) // TODO examine branches
          case Subset(_, _, _, _) => default.remap(env, in) // TODO examine branches

          case Map(src, func) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Map(src, remapIndicesInFunc(func, repl))
            M.get >>= (st => haltRemap(st.fold(in)(indexMapping >>> replacement)))

          case Filter(src, func) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Filter(src, remapIndicesInFunc(func, repl))
            M.get ∘ (_.fold(in)(indexMapping >>> replacement))

          case Sort(src, bucket0, order0) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Sort(
                src,
                remapIndicesInFunc(bucket0, repl),
                order0.map(_.leftMap(remapIndicesInFunc(_, repl))))
            M.get ∘ (_.fold(in)(indexMapping >>> replacement))

          case Unreferenced() => default.remap(env, in)
        }
    }
}

class PAFindRemap[T[_[_]]: BirecursiveT, F[_]: Functor] {
  import PATypes._

  type ArrayEnv[G[_], A] = EnvT[KnownIndices, G, A]

  /** Given an input, we accumulate state and annotate the focus.
    *
    * The state collects the used indices and indicates if the dereferenced
    * array can be pruned. For example, if we deref an array non-statically, we
    * cannot prune it.
    *
    * If the focus is an array that can be pruned, the annotatation is set to
    * the state. Else the annotation is set to `None`.
    */
  def findIndices[M[_]](implicit M: MonadState[M, KnownIndices], P: PruneArrays[F])
      : CoalgebraM[M, ArrayEnv[F, ?], T[F]] = tqs => {
    val gtg = tqs.project
    P.find(gtg) ∘ (newEnv => EnvT((newEnv, gtg)))
  }

  /** Given an annotated input, we produce an output with state.
    *
    * If the previous state provides indices, we remap array dereferences accordingly.
    *
    * If an array has an associated environment, we update the state
    * to be the environment and prune the array.
    */
  def remapIndices[M[_]](implicit M: MonadState[M, KnownIndices], P: PruneArrays[F])
      : AlgebraM[M, ArrayEnv[F, ?], T[F]] = arrenv => {
    val (env, qs): (KnownIndices, F[T[F]]) = arrenv.run

    P.remap(env, qs) ∘ (_.embed)
  }
}
