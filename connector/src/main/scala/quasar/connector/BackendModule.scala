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

trait BackendModule[Config] {
  type QS[T[_[_]]] <: CoM
  type QSM[T[_[_]], A] = QS[T]#M[A]

  type Repr
  type M[A]

  def FunctorQSM[T[_[_]]]: Functor[QSM[T, ?]]
  private final implicit def _FunctorQSM[T[_[_]]] = FunctorQSM[T]

  def DelayRenderTreeQSM[T[_[_]]]: Delay[RenderTree, QSM[T, ?]]
  private final implicit def _DelayRenderTreeQSM[T[_[_]]]: Delay[RenderTree, QSM[T, ?]] = DelayRenderTreeQSM

  def ExtractPathQSM[T[_[_]]]: ExtractPath[QSM[T, ?], APath]
  private final implicit def _ExtractPathQSM[T[_[_]]]: ExtractPath[QSM[T, ?], APath] = ExtractPathQSM

  def QSCoreInject[T[_[_]]]: QScriptCore[T, ?] :<: QSM[T, ?]
  private final implicit def _QSCoreInject[T[_[_]]] = QSCoreInject[T]

  def MonadM: Monad[M]
  private final implicit def _MonadM = MonadM

  def MonadFsErrM: MonadFsErr[M]
  private final implicit def _MonadFsErrM = MonadFsErrM

  def PhaseResultTellM: PhaseResultTell[M]
  private final implicit def _PhaseResultTellM = PhaseResultTellM

  def PhaseResultListenM: PhaseResultListen[M]
  private final implicit def _PhaseResultListenM = PhaseResultListenM

  def UnirewriteT[T[_[_]]]: Unirewrite[T, QS[T]]
  private final implicit def _UnirewriteT[T[_[_]]] = UnirewriteT[T]

  def UnicoalesceCap[T[_[_]]]: Unicoalesce.Capture[T, QS[T]]
  private final implicit def _UnicoalesceCap[T[_[_]]] = UnicoalesceCap[T]

  // TODO enrich to something like EitherT so we can factor out errors that are concerning
  // maybe PhysicalErr somethingorother?
  type Final[A] = Task[A]

  def compile(config: Config, uri: ConnectionUri): FileSystemDef.DefErrT[Final, (M ~> Final, Final[Unit])]

  val Type: FileSystemType

  final def definition(config: Config): FileSystemDef[Final] = FileSystemDef fromPF {
    case (Type, uri) =>
      compile(config, uri) map {
        case (int, close) =>
          FileSystemDef.DefinitionResult(int compose fsInterpreter, close)
      }
  }

  private def fsInterpreter: FileSystem ~> M = {
    def rePhaseify[A](back: M[A]) =
      MonadListen_[M, PhaseResults].listen(back.attempt).map(_.swap)

    val qfInter: QueryFile ~> M = λ[QueryFile ~> M] {
      case QueryFile.ExecutePlan(lp, out) =>
        val back = lpToRepr(lp).map(_.repr).flatMap(r => QueryFileModule.executePlan(r, out))
        rePhaseify(back)

      case QueryFile.EvaluatePlan(lp) =>
        val back = lpToRepr(lp).map(_.repr).flatMap(r => QueryFileModule.evaluatePlan(r))
        rePhaseify(back)

      case QueryFile.More(h) => QueryFileModule.more(h).attempt
      case QueryFile.Close(h) => QueryFileModule.close(h)

      case QueryFile.Explain(lp) =>
        val back = for {
          pp <- lpToRepr(lp)
          explanation <- QueryFileModule.explain(pp.repr)
        } yield ExecutionPlan(Type, explanation, pp.paths)

        rePhaseify(back)

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

  final def lpToRepr[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT: OrderT](
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

final case class PhysicalPlan[R](repr: R, paths: ISet[APath])
