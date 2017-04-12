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
import quasar.fp.free._
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
  type QSM[T[_[_]], A] = QS[T]#M[A]

  private final implicit def _FunctorQSM[T[_[_]]] = FunctorQSM[T]
  private final implicit def _DelayRenderTreeQSM[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT]: Delay[RenderTree, QSM[T, ?]] = DelayRenderTreeQSM
  private final implicit def _ExtractPathQSM[T[_[_]]: RecursiveT]: ExtractPath[QSM[T, ?], APath] = ExtractPathQSM
  private final implicit def _QSCoreInject[T[_[_]]] = QSCoreInject[T]
  private final implicit def _MonadM = MonadM
  private final implicit def _MonadFsErrM = MonadFsErrM
  private final implicit def _PhaseResultTellM = PhaseResultTellM
  private final implicit def _PhaseResultListenM = PhaseResultListenM
  private final implicit def _UnirewriteT[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT] = UnirewriteT[T]
  private final implicit def _UnicoalesceCap[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT] = UnicoalesceCap[T]

  final def definition[Task] = FileSystemDef fromPF {
    case (Type, uri) =>
      compile(uri) map {
        case (int, close) =>
          FileSystemDef.DefinitionResult(int compose fsInterpreter, close)
      }
  }

  private final def fsInterpreter: FileSystem ~> M = {
    def attemptListen[A](back: M[A]) =
      MonadListen_[M, PhaseResults].listen(back.attempt).map(_.swap)

    val qfInter: QueryFile ~> M = λ[QueryFile ~> M] {
      case QueryFile.ExecutePlan(lp, out) =>
        val back = lpToRepr(lp).map(_.repr).flatMap(r => QueryFileModule.executePlan(r, out))
        attemptListen(back)

      case QueryFile.EvaluatePlan(lp) =>
        val back = lpToRepr(lp).map(_.repr).flatMap(r => QueryFileModule.evaluatePlan(r))
        attemptListen(back)

      case QueryFile.More(h) => QueryFileModule.more(h).attempt
      case QueryFile.Close(h) => QueryFileModule.close(h)

      case QueryFile.Explain(lp) =>
        val back = for {
          pp <- lpToRepr(lp)
          explanation <- QueryFileModule.explain(pp.repr)
        } yield ExecutionPlan(Type, explanation, pp.paths)

        attemptListen(back)

      case QueryFile.ListContents(dir) => QueryFileModule.listContents(dir).attempt
      case QueryFile.FileExists(file) => QueryFileModule.fileExists(file)
    }

    val rfInter: ReadFile ~> M = λ[ReadFile ~> M] {
      case ReadFile.Open(file, offset, limit) =>
        ReadFileModule.open(file, offset, limit).attempt

      case ReadFile.Read(h) => ReadFileModule.read(h).attempt
      case ReadFile.Close(h) => ReadFileModule.close(h)
    }

    val wfInter: WriteFile ~> M = λ[WriteFile ~> M] {
      case WriteFile.Open(file) => WriteFileModule.open(file).attempt
      case WriteFile.Write(h, chunk) => WriteFileModule.write(h, chunk)
      case WriteFile.Close(h) => WriteFileModule.close(h)
    }

    val mfInter: ManageFile ~> M = λ[ManageFile ~> M] {
      case ManageFile.Move(scenario, semantics) =>
        ManageFileModule.move(scenario, semantics).attempt

      case ManageFile.Delete(path) => ManageFileModule.delete(path).attempt
      case ManageFile.TempFile(near) => ManageFileModule.tempFile(near).attempt
    }

    qfInter :+: rfInter :+: wfInter :+: mfInter
  }

  final def lpToRepr[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT](
      lp: T[LogicalPlan]): M[PhysicalPlan[Repr]] = {

    type QSR[A] = QScriptRead[T, A]

    def logPhase(pr: PhaseResult): M[Unit] =
      MonadTell_[M, PhaseResults].tell(Vector(pr))

    val lc: DiscoverPath.ListContents[M] =
      QueryFileModule.listContents(_)

    val R = new Rewrite[T]

    for {
      qs <- QueryFile.convertToQScriptRead[T, M, QSR](lc)(lp)
      shifted <- Unirewrite[T, QS[T], M](R, lc).apply(qs)

      _ <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted))

      optimized =
        shifted.transHylo(R.optimize(reflNT[QSM[T, ?]]), Unicoalesce.Capture[T, QS[T]].run)

      _ <- logPhase(PhaseResult.tree("QScript (Optimized)", optimized))

      main <- plan(optimized)
      inputs = optimized.cata(ExtractPath[QSM[T, ?], APath].extractPath[DList])
    } yield PhysicalPlan(main, ISet.fromFoldable(inputs))
  }

  // everything abstract below this line

  type QS[T[_[_]]] <: CoM

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]]

  type Config
  def config: Config

  type Repr
  type M[A]

  def FunctorQSM[T[_[_]]]: Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT]: Delay[RenderTree, QSM[T, ?]]
  def ExtractPathQSM[T[_[_]]: RecursiveT]: ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]]: QScriptCore[T, ?] :<: QSM[T, ?]
  def MonadM: Monad[M]
  def MonadFsErrM: MonadFsErr[M]
  def PhaseResultTellM: PhaseResultTell[M]
  def PhaseResultListenM: PhaseResultListen[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT]: Unirewrite[T, QS[T]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT]: Unicoalesce.Capture[T, QS[T]]

  def compile(uri: ConnectionUri): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])]

  val Type: FileSystemType

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT: OrderT](
      cp: T[QSM[T, ?]]): M[Repr]

  trait QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): M[AFile]
    def evaluatePlan(repr: Repr): M[ResultHandle]
    def more(h: ResultHandle): M[Vector[Data]]
    def close(h: ResultHandle): M[Unit]
    def explain(repr: Repr): M[String]
    def listContents(dir: ADir): M[Set[PathSegment]]
    def fileExists(file: AFile): M[Boolean]
  }

  def QueryFileModule: QueryFileModule

  trait ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): M[ReadHandle]
    def read(h: ReadHandle): M[Vector[Data]]
    def close(h: ReadHandle): M[Unit]
  }

  def ReadFileModule: ReadFileModule

  trait WriteFileModule {
    import WriteFile._

    def open(file: AFile): M[WriteHandle]
    def write(h: WriteHandle, chunk: Vector[Data]): M[Vector[FileSystemError]]
    def close(h: WriteHandle): M[Unit]
  }

  def WriteFileModule: WriteFileModule

  trait ManageFileModule {
    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): M[Unit]
    def delete(path: APath): M[Unit]
    def tempFile(near: APath): M[AFile]
  }

  def ManageFileModule: ManageFileModule
}
