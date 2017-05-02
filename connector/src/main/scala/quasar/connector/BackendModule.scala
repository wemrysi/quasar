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

  type ErrorMessages = NonEmptyList[String]

  private final implicit def _FunctorQSM[T[_[_]]] = FunctorQSM[T]
  private final implicit def _DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Delay[RenderTree, QSM[T, ?]] = DelayRenderTreeQSM
  private final implicit def _ExtractPathQSM[T[_[_]]: RecursiveT]: ExtractPath[QSM[T, ?], APath] = ExtractPathQSM
  private final implicit def _QSCoreInject[T[_[_]]] = QSCoreInject[T]
  private final implicit def _MonadM = MonadM
  private final implicit def _MonadFsErrM = MonadFsErrM
  private final implicit def _PhaseResultTellM = PhaseResultTellM
  private final implicit def _PhaseResultListenM = PhaseResultListenM
  private final implicit def _UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = UnirewriteT[T]
  private final implicit def _UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = UnicoalesceCap[T]

  final def definition = FileSystemDef fromPF {
    case (Type, uri) =>
      parseConfig(uri).leftMap(_.left[EnvironmentError]) flatMap { cfg =>
        compile(cfg) map {
          case (int, close) =>
            val runK = λ[Kleisli[M, Config, ?] ~> M](_.run(cfg))
            FileSystemDef.DefinitionResult(int compose runK compose fsInterpreter, close)
        }
      }
  }

  private final def fsInterpreter: FileSystem ~> Kleisli[M, Config, ?] = {
    type Back[A] = Kleisli[M, Config, A]

    def attemptListen[A](back: Back[A]) =
      MonadListen_[Back, PhaseResults].listen(back.attempt).map(_.swap)

    val qfInter: QueryFile ~> Back = λ[QueryFile ~> Back] {
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

    val rfInter: ReadFile ~> Back = λ[ReadFile ~> Back] {
      case ReadFile.Open(file, offset, limit) =>
        ReadFileModule.open(file, offset, limit).attempt

      case ReadFile.Read(h) => ReadFileModule.read(h).attempt
      case ReadFile.Close(h) => ReadFileModule.close(h)
    }

    val wfInter: WriteFile ~> Back = λ[WriteFile ~> Back] {
      case WriteFile.Open(file) => WriteFileModule.open(file).attempt
      case WriteFile.Write(h, chunk) => WriteFileModule.write(h, chunk)
      case WriteFile.Close(h) => WriteFileModule.close(h)
    }

    val mfInter: ManageFile ~> Back = λ[ManageFile ~> Back] {
      case ManageFile.Move(scenario, semantics) =>
        ManageFileModule.move(scenario, semantics).attempt

      case ManageFile.Delete(path) => ManageFileModule.delete(path).attempt
      case ManageFile.TempFile(near) => ManageFileModule.tempFile(near).attempt
    }

    qfInter :+: rfInter :+: wfInter :+: mfInter
  }

  final def lpToRepr[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      lp: T[LogicalPlan]): Kleisli[M, Config, PhysicalPlan[Repr]] = {

    type QSR[A] = QScriptRead[T, A]

    def logPhase(pr: PhaseResult): M[Unit] =
      MonadTell_[M, PhaseResults].tell(Vector(pr))

    val lc: DiscoverPath.ListContents[Kleisli[M, Config, ?]] =
      QueryFileModule.listContents(_)

    val R = new Rewrite[T]
    val O = new Optimize[T]

    for {
      qs <- QueryFile.convertToQScriptRead[T, Kleisli[M, Config, ?], QSR](lc)(lp)
      shifted <- Unirewrite[T, QS[T], Kleisli[M, Config, ?]](R, lc).apply(qs)

      _ <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted)).liftM[Kleisli[?[_], Config, ?]]

      optimized =
        shifted.transHylo(O.optimize(reflNT[QSM[T, ?]]), Unicoalesce.Capture[T, QS[T]].run)

      _ <- logPhase(PhaseResult.tree("QScript (Optimized)", optimized)).liftM[Kleisli[?[_], Config, ?]]

      main <- plan(optimized).liftM[Kleisli[?[_], Config, ?]]
      inputs = optimized.cata(ExtractPath[QSM[T, ?], APath].extractPath[DList])
    } yield PhysicalPlan(main, ISet.fromFoldable(inputs))
  }

  // everything abstract below this line

  type QS[T[_[_]]] <: CoM

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]]

  type Repr
  type M[A]

  def FunctorQSM[T[_[_]]]: Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Delay[RenderTree, QSM[T, ?]]
  def ExtractPathQSM[T[_[_]]: RecursiveT]: ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]]: QScriptCore[T, ?] :<: QSM[T, ?]
  def MonadM: Monad[M]
  def MonadFsErrM: MonadFsErr[M]
  def PhaseResultTellM: PhaseResultTell[M]
  def PhaseResultListenM: PhaseResultListen[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Unirewrite[T, QS[T]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Unicoalesce.Capture[T, QS[T]]

  type Config
  def parseConfig(uri: ConnectionUri): EitherT[Task, ErrorMessages, Config]

  def compile(cfg: Config): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])]

  val Type: FileSystemType

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): M[Repr]

  trait QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): Kleisli[M, Config, AFile]
    def evaluatePlan(repr: Repr): Kleisli[M, Config, ResultHandle]
    def more(h: ResultHandle): Kleisli[M, Config, Vector[Data]]
    def close(h: ResultHandle): Kleisli[M, Config, Unit]
    def explain(repr: Repr): Kleisli[M, Config, String]
    def listContents(dir: ADir): Kleisli[M, Config, Set[PathSegment]]
    def fileExists(file: AFile): Kleisli[M, Config, Boolean]
  }

  def QueryFileModule: QueryFileModule

  trait ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Kleisli[M, Config, ReadHandle]
    def read(h: ReadHandle): Kleisli[M, Config, Vector[Data]]
    def close(h: ReadHandle): Kleisli[M, Config, Unit]
  }

  def ReadFileModule: ReadFileModule

  trait WriteFileModule {
    import WriteFile._

    def open(file: AFile): Kleisli[M, Config, WriteHandle]
    def write(h: WriteHandle, chunk: Vector[Data]): Kleisli[M, Config, Vector[FileSystemError]]
    def close(h: WriteHandle): Kleisli[M, Config, Unit]
  }

  def WriteFileModule: WriteFileModule

  trait ManageFileModule {
    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): Kleisli[M, Config, Unit]
    def delete(path: APath): Kleisli[M, Config, Unit]
    def tempFile(near: APath): Kleisli[M, Config, AFile]
  }

  def ManageFileModule: ManageFileModule
}
