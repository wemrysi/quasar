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

package quasar
package connector

import slamdata.Predef._
import quasar.common._
import quasar.contrib.pathy._
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.fp.numeric.{Natural, Positive}
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.syntax.all._
import scalaz.concurrent.Task

trait BackendModule {
  type QS[T[_[_]]] <: CoM
  type QSM[T[_[_]], A] = QS[T]#M[A]

  type Config

  type Repr
  type M[A]

  def FunctorQSM[T[_[_]]]: Functor[QSM[T, ?]]
  private final implicit def _FunctorQSM[T[_[_]]] = FunctorQSM[T]

  def QSCoreInject[T[_[_]]]: QScriptCore[T, ?] :<: QSM[T, ?]
  private final implicit def _QSCoreInject[T[_[_]]] = QSCoreInject[T]

  def MonadM: Monad[M]
  private final implicit def _MonadM = MonadM

  def MonadFsErrM: MonadFsErr[M]
  private final implicit def _MonadFsErrM = MonadFsErrM

  def PhaseResultTellM: PhaseResultTell[M]
  private final implicit def _PhaseResultTellM = PhaseResultTellM

  def UnirewriteT[T[_[_]]]: Unirewrite[T, QS[T]]
  private final implicit def _UnirewriteT[T[_[_]]] = UnirewriteT[T]

  def UnicoalesceCap[T[_[_]]]: Unicoalesce.Capture[T, QS[T]]
  private final implicit def _UnicoalesceCap[T[_[_]]] = UnicoalesceCap[T]

  def compile: M ~> Task

  final def definition(config: Config): FileSystemDef[Task] = ???

  final def lpToRepr[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT: OrderT](
      lp: T[LogicalPlan]): M[Repr] = {

    type QSR[A] = QScriptRead[T, A]

    val lc: DiscoverPath.ListContents[M] =
      (QueryFileModule.listContents(_)).andThen(MonadError_[M, FileSystemError].unattempt(_))

    val R = new Rewrite[T]

    for {
      qs <- QueryFile.convertToQScriptRead[T, M, QSR](lc)(lp)
      shifted <- Unirewrite[T, QS[T], M](R, lc).apply(qs)

      // _ <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted))

      optimized =
        shifted.transHylo(R.optimize(reflNT[QSM[T, ?]]), Unicoalesce.Capture[T, QS[T]].run)

      // _ <- logPhase(PhaseResult.tree("QScript (Optimized)", optimized))

      main <- plan(optimized)
    } yield main
  }

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): M[Repr]

  trait QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): M[(PhaseResults, FileSystemError \/ AFile)]
    def evaluatePlan(repr: Repr): M[(PhaseResults, FileSystemError \/ ResultHandle)]
    def more(h: ResultHandle): M[FileSystemError \/ Vector[Data]]
    def close(h: ResultHandle): M[Unit]
    def explain(repr: Repr): M[(PhaseResults, FileSystemError \/ ExecutionPlan)]
    def listContents(dir: ADir): M[FileSystemError \/ Set[PathSegment]]
    def fileExists(file: AFile): M[Boolean]
  }

  def QueryFileModule: QueryFileModule

  trait ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): M[FileSystemError \/ ReadHandle]
    def read(h: ReadHandle): M[FileSystemError \/ Vector[Data]]
    def close(h: ReadHandle): M[Unit]
  }

  def ReadFileModule: ReadFileModule

  trait WriteFileModule {
    import WriteFile._

    def open(file: AFile): M[FileSystemError \/ WriteHandle]
    def write(h: WriteHandle, chunk: Vector[Data]): M[Vector[FileSystemError]]
    def close(h: WriteHandle): M[Unit]
  }

  def WriteFileModule: WriteFileModule

  trait ManageFileModule {
    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): M[FileSystemError \/ Unit]
    def delete(path: APath): M[FileSystemError \/ Unit]
    def tempFile(near: APath): M[FileSystemError \/ AFile]
  }

  def ManageFileModule: ManageFileModule
}
