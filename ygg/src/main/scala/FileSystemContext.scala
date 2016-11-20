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

package quasar.fs

import quasar.Predef._
import quasar.common.PhaseResults
import quasar.contrib.pathy._
import quasar.fp.TaskRef
import quasar.fs.mount._, FileSystemDef._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import quasar.frontend.logicalplan.{ LogicalPlan => LP }
import matryoshka.Fix

/** This trait is intended as a basis for filesystem implementations.
 *  It encapsulates the most common types, utility methods, and implicit
 *  value liftings to minimize boilerplate and duplication at the leaves.
 *
 *  There are no constraints on F[_] except on the abstract class Impl,
 *  a convenience class which accepts the Applicative[F] from its parent.
 *  The other standard entry point is Free, where the applicative instance
 *  is the free monad.
 */
trait FileSystemContext[F0[_]] extends EitherTContextLeft[F0, FileSystemError] with FileSystemTypes[F0] {
  import FileSystemError._
  import quasar.Planner.UnsupportedPlan

  type F[A] = F0[A]

  def unknownPath(p: APath): FileSystemError    = pathErr(PathError pathNotFound p)
  def unknownPlan(lp: Fix[LP]): FileSystemError = planningFailed(lp, UnsupportedPlan(lp.unFix, None))
  def makeDirList(names: PathSegment*): DirList = names.toSet
}
object FileSystemContext {
  abstract class Impl[F[_]](implicit F: Applicative[F]) extends FileSystemContext[F] {
    implicit protected val applicative = F
  }
  trait Free[F0[_]] extends FileSystemContext[scalaz.Free[F0, ?]] with MonadicContext[scalaz.Free[F0, ?]] {
    implicit protected val applicative: Monad[F] = scalaz.Free.freeMonad[F0]
  }
}

/** Here we take FileSystemContext in a "unified implementation" direction,
 *  to create the simplest possible starting basis. We inherit all four of the
 *  FileSystem traits and leave it to subclasses to implement the abstract
 *  methods to which the algebra dispatches.
 */
abstract class UnifiedFileSystem[F[_]: Applicative]
      extends FileSystemContext.Impl[F]
         with QueryFileSystem[F]
         with ReadFileSystem[F]
         with WriteFileSystem[F]
         with ManageFileSystem[F] {

  protected implicit def liftPhaseErr[A](err: FileSystemError): FPLR[A] = point(Vector() -> err.left)

  /** The coproduct elements which we've inherited from the various *FileSystem traits,
   *  now dispatching to all the abstract methods we have also inherited.
   */
  def fileSystem: FileSystem ~> F = interpretFileSystem(queryFile, readFile, writeFile, manageFile)

  def definition(FsType: FileSystemType): FileSystemDef[F] = FileSystemDef fromPF {
    case (FsType, init) => EitherT right point(DefinitionResult(fileSystem, ()))
  }
}

/** Convenience methods for a filesystem based on the state monad.
 */
trait StatefulFileSystem {
  type S
  def fileSystem: FileSystem ~> F

  type F[A]          = State[S, A]
  type FOpt[A]       = F[Option[A]]
  type StateLens[A]  = S @> A
  type StateLensP[A] = StateLens[Option[A]]

  def fullState[A](f: S => (S, A)): F[A]                = State(f)
  def onState[A](f: S => A): F[A]                       = State gets f
  def modifyState(f: S => S): F[Unit]                   = State(s => f(s) -> (()))
  def runStatefully(init: S): Task[F ~> Task]           = runInspect(init).map(_._1)
  def runFs(init: S): Task[FileSystem ~> Task]          = runStatefully(init) map (_ compose fileSystem)
  def runInspect(init: S): Task[(F ~> Task) -> Task[S]] = TaskRef(init) map (r => λ[F ~> Task](r modifyS _.run) -> r.read)
}

/** This is a parent for an object which builds some UnifiedFileSystem,
 *  offering various convenience methods.
 */
trait UnifiedFileSystemBuilder {
  def FsType: FileSystemType
  def apply[F[_]: Applicative] : UnifiedFileSystem[F]

  def freeFs[F[_]]: UnifiedFileSystem[Free[F, ?]] = apply[Free[F, ?]]
  def queryFile[F[_]]: QueryFile ~> Free[F, ?]    = freeFs[F].queryFile
  def readFile[F[_]]: ReadFile ~> Free[F, ?]      = freeFs[F].readFile
  def writeFile[F[_]]: WriteFile ~> Free[F, ?]    = freeFs[F].writeFile
  def manageFile[F[_]]: ManageFile ~> Free[F, ?]  = freeFs[F].manageFile
  def fileSystem[F[_]]: FileSystem ~> Free[F, ?]  = freeFs[F].fileSystem
  def definition[F[_]]: FileSystemDef[Free[F, ?]] = freeFs[F].definition(FsType)
}

/** Standardized aliases for filesystem implementations.
*/
trait FileSystemTypes[F[_]] extends EitherTypes[F, FileSystemError] with FileSystemIndependentTypes {
  type FPLR[A] = F[PhaseResults -> LR[A]]
}
trait FileSystemIndependentTypes {
  type Errors  = Vector[FileSystemError]
  type DirList = Set[PathSegment]
  type Chunks  = Vector[quasar.Data]

  type WHandle = WriteFile.WriteHandle
  type RHandle = ReadFile.ReadHandle
  type QHandle = QueryFile.ResultHandle

  val WHandle = WriteFile.WriteHandle
  val RHandle = ReadFile.ReadHandle
  val QHandle = QueryFile.ResultHandle

  type FileMap[+V]  = Map[AFile, V]
  type QueryMap[+V] = Map[QHandle, V]
  type ReadMap[+V]  = Map[RHandle, V]
  type WriteMap[+V] = Map[WHandle, V]
  type PlanMap[+V]  = Map[Fix[LP], V]
}
object FileSystemIndependentTypes extends FileSystemIndependentTypes

/** These traits are not strictly necessary to implement a filesystem which is
 *  defined as a coproduct of natural transformations. However they simplify the
 *  implementation process quite a bit by bringing all the types closer to ground.
 *  They implement the natural transformations concretely, dispatching the
 *  various cases to abstract methods which must be implemented.
 */
trait QueryFileSystem[F[_]] extends FileSystemTypes[F] {
  import QueryFile._

  def closeQ(rh: QHandle): F[Unit]
  def evaluate(lp: Fix[LP]): FPLR[QHandle]
  def execute(lp: Fix[LP], out: AFile): FPLR[AFile]
  def exists(file: AFile): F[Boolean]
  def explain(lp: Fix[LP]): FPLR[ExecutionPlan]
  def list(dir: ADir): FLR[DirList]
  def more(rh: QHandle): FLR[Chunks]

  val queryFile = λ[QueryFile ~> F] {
    case ExecutePlan(lp, out) => execute(lp, out)
    case EvaluatePlan(lp)     => evaluate(lp)
    case Explain(lp)          => explain(lp)
    case More(rh)             => more(rh)
    case ListContents(dir)    => list(dir)
    case Close(rh)            => closeQ(rh)
    case FileExists(file)     => exists(file)
  }
}
trait ReadFileSystem[F[_]] extends FileSystemTypes[F] {
  import ReadFile._
  import quasar.fp.numeric._

  def openForRead(file: AFile, offset: Natural, limit: Option[Positive]): FLR[RHandle]
  def read(fh: RHandle): FLR[Chunks]
  def closeR(fh: RHandle): F[Unit]

  val readFile = λ[ReadFile ~> F] {
    case Open(file, offset, limit) => openForRead(file, offset, limit)
    case Read(fh)                  => read(fh)
    case Close(fh)                 => closeR(fh)
  }
}
trait WriteFileSystem[F[_]] extends FileSystemTypes[F] {
  import WriteFile._

  def openForWrite(file: AFile): FLR[WHandle]
  def write(fh: WHandle, chunks: Chunks): F[Errors]
  def closeW(fh: WHandle): F[Unit]

  val writeFile = λ[WriteFile ~> F] {
    case Open(file)        => openForWrite(file)
    case Write(fh, chunks) => write(fh, chunks)
    case Close(fh)         => closeW(fh)
  }
}
trait ManageFileSystem[F[_]] extends FileSystemTypes[F] {
  import ManageFile._
  type MoveSemantics = ManageFile.MoveSemantics

  def createTempFile(near: APath): FLR[AFile]
  def deletePath(path: APath): FLR[Unit]
  def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]
  def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics): FLR[Unit]

  val manageFile = λ[ManageFile ~> F] {
    case Move(scenario, semantics) => scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
    case Delete(path)              => deletePath(path)
    case TempFile(path)            => createTempFile(path)
  }
}
