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

package quasar.physical.jsonfile

import quasar.{ PhaseResult, PhaseResults }
import quasar.Predef._
import quasar.fp._, free._, numeric._
import quasar.fs._
import quasar.fs.mount._, FileSystemDef._
import quasar.effect._
import FileSystemError._
import quasar.Planner.UnsupportedPlan
import pathy.Path._
import quasar.contrib.pathy._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import FileSystemIndependentTypes._
import ManageFile.MoveSemantics

import ygg.table._
import ygg.json.JValue
// import matryoshka._
// import Recursive.ops._

// XXX tests which pass too easily:
//
// [info]     + filter on date part, where the field isn't a timestamp [./temporal/invalidDateFilter.test]
// [info]     + reduce a literal set with negatives [./literalReductionWithNegative.test]
// [info]     + regex on non-string field [./guardedExpression.test]
//
// Planner Errors:
//
// fcc FuncApply(name: String, expected: String, actual: String)
// fcc InternalError(message: String)
// fcc NoFilesFound(dirs: List[ADir])
// fcc NonRepresentableData(data: Data)
// fcc NonRepresentableEJson(data: String)
// fcc NonRepresentableInJS(value: String)
// fcc ObjectIdFormatError(str: String)
// fcc PlanPathError(error: PathError)
// fcc UnboundVariable(name: Symbol)
// fcc UnsupportedFunction(name: String, hint: Option[String])
// fcc UnsupportedJS(value: String)
// fcc UnsupportedJoinCondition(cond: Fix[LogicalPlan])
// fcc UnsupportedPlan(plan: LogicalPlan[_], hint: Option[String])

package object fs extends fs.FilesystemEffect {
  val FsType = FileSystemType("jsonfile")

  val tracingProp: Option[String] = scala.sys.props get "ygg.trace" map (".*(" + _ + ").*")

  def isTracing(label: String): Boolean = tracingProp exists (label matches _)

  implicit class KVSOps[K, V, S[_]](val kvs: KVInject[K, V, S]) {
    implicit private def kvs_ = kvs
    private def Ops = KeyValueStore.Ops[K, V, S]

    def keys                  = Ops.keys
    def contains(key: K)      = Ops contains key
    def delete(key: K)        = Ops delete key
    def put(key: K, value: V) = Ops.put(key, value)
  }

  type FH = Table
  type RH = Table
  type WH = Long
  type QH = JValue

  def emptyFile(): FH = ColumnarTable.empty

  def create[S[_]](file: AFile, uid: Long)(implicit KVF: KVFile[S], KVW: KVWrite[S]) = {
    val wh = WHandle(file, uid)
    for {
      _ <- KVF.put(file, emptyFile())
      _ <- KVW.put(wh, 0)
    } yield wh
  }

  implicit def showPath: Show[APath]      = Show shows (posixCodec printPath _)
  implicit def showRHandle: Show[RHandle] = Show shows (r => "ReadHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showWHandle: Show[WHandle] = Show shows (r => "WriteHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showFixPlan: Show[FixPlan] = Show shows (lp => FPlan("", lp).toString)

  def tmpName(n: Long): String                  = s"__quasar.ygg$n"
  def unknownPath(p: APath): FileSystemError    = pathErr(PathError pathNotFound p)
  def unknownPlan(lp: FixPlan): FileSystemError = planningFailed(lp, UnsupportedPlan(lp.unFix, None))
  def makeDirList(names: PathSegment*): DirList = names.toSet

  def trace[A: Show](label: String)(value: A): A = tracing(label, value)(value)
  def tracing[A: Show, B](label: String, value: A)(expr: B): B = {
    if (isTracing(label))
      println(label + ": " + value.show)

    expr
  }

  def queryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]): QueryFile ~> Free[S, ?] = new FsAlgebras[S].queryFile
  def readFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]): ReadFile ~> Free[S, ?]    = new FsAlgebras[S].readFile
  def writeFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]): WriteFile ~> Free[S, ?] = new FsAlgebras[S].writeFile
  def manageFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]): ManageFile ~> Free[S, ?]                = new FsAlgebras[S].manageFile
}

package fs {
  class FsAlgebras[S[_]] extends STypes[S] {
    def phaseResults(lp: FixPlan): FS[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", "<no description>"))
    def nextLong(implicit MS: MonotonicSeq :<: S)   = MonotonicSeq.Ops[S].next

    def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]    = ()
    def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics): FLR[Unit] = ()

    def createTempFile(near: APath, uid: Long): LR[AFile] = {
      val name = file(tmpName(uid))
      refineType(near).fold(
        _ </> name,
        f => fileParent(f) </> name
      ).right
    }

    def manageFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]) = λ[ManageFile ~> FS] {
      case ManageFile.Move(scenario, semantics) => tracing("Move", scenario.toString -> semantics.toString)(scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics)))
      case ManageFile.Delete(path)              => tracing("Delete", path)(refineType(path).fold(_ => Unimplemented, KVF delete _ map (_ => ().right)))
      case ManageFile.TempFile(path)            => tracing("TempFile", path)(nextLong map (uid => createTempFile(path, uid)))
    }
    def writeFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]) = λ[WriteFile ~> FS] {
      case WriteFile.Open(file)        => tracing("Write.Open", file)(nextLong flatMap (uid => create[S](file, uid) map (wh => wh.right)))
      case WriteFile.Write(fh, chunks) => tracing("Write", fh)(Vector())
      case WriteFile.Close(fh)         => tracing("Write.Close", fh)(KVW delete fh)
    }
    def readFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]) = λ[ReadFile ~> FS] {
      case ReadFile.Open(file, offset, limit) => tracing("Read.Open", file)(nextLong map (uid => RHandle(file, uid).right))
      case ReadFile.Read(fh)                  => tracing("Read", fh)(Vector())
      case ReadFile.Close(fh)                 => tracing("Read.Close", fh)(KVR delete fh)
    }
    def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) = λ[QueryFile ~> FS] {
      case QueryFile.ExecutePlan(lp, out) => tracing("Execute", lp -> out)(phaseResults(lp) tuple \/-(out))
      case QueryFile.EvaluatePlan(lp)     => tracing("Evaluate", lp)((phaseResults(lp) |@| nextLong)((ph, uid) => ph -> \/-(QHandle(uid))))
      case QueryFile.Explain(lp)          => tracing("Explain", lp)(phaseResults(lp) tuple ExecutionPlan(FsType, "..."))
      case QueryFile.More(rh)             => tracing("More", rh)(Vector())
      case QueryFile.ListContents(dir)    => tracing("List", dir)(KVF.keys map (_ flatMap pathName) map (xs => makeDirList(xs: _*).right))
      case QueryFile.Close(fh)            => tracing("Query.Close", fh)(KVQ delete fh)
      case QueryFile.FileExists(file)     => tracing("Exists", file)(KVF contains file)
    }
  }

  trait STypes[S[_]] extends EitherTContextLeft[Free[S, ?], FileSystemError] {
    implicit protected val applicative: Applicative[FS] = scalaz.Free.freeMonad[S]

    type FS[A]  = Free[S, A]
    type FSUnit = FS[Unit]
    type FSBool = FS[Boolean]
  }

  trait FilesystemEffect {
    val FsType: FileSystemType

    type FH // file map values
    type RH // read handle map values
    type WH // write handle map values
    type QH // query handle map values

    def emptyFile(): FH

    type KVFile[S[_]]  = KVInject[AFile, FH, S]
    type KVRead[S[_]]  = KVInject[RHandle, RH, S]
    type KVWrite[S[_]] = KVInject[WHandle, WH, S]
    type KVQuery[S[_]] = KVInject[QHandle, QH, S]

    type AsTask[M[X]]         = Task[M ~> Task]
    type KVInject[K, V, S[_]] = KeyValueStore[K, V, ?] :<: S

    def kvEmpty[K, V] : AsTask[KeyValueStore[K, V, ?]]   = KeyValueStore.impl.empty[K, V]
    def kvOps[K, V, S[_]](implicit z: KVInject[K, V, S]) = KeyValueStore.Ops[K, V, S]


    def queryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]): QueryFile ~> Free[S, ?]
    def readFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]): ReadFile ~> Free[S, ?]
    def writeFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]): WriteFile ~> Free[S, ?]
    def manageFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]): ManageFile ~> Free[S, ?]

    type Eff[A] = (
          Task
      :\: KeyValueStore[AFile, FH, ?]
      :\: KeyValueStore[RHandle, RH, ?]
      :\: KeyValueStore[WHandle, WH, ?]
      :\: KeyValueStore[QHandle, QH, ?]
      :/: MonotonicSeq
    )#M[A]

    def initialEffect(uri: ConnectionUri): AsTask[Eff] = (
          (Task delay reflNT[Task])
      |@| kvEmpty[AFile, FH]
      |@| kvEmpty[RHandle, RH]
      |@| kvEmpty[WHandle, WH]
      |@| kvEmpty[QHandle, QH]
      |@| MonotonicSeq.fromZero
    )(_ :+: _ :+: _ :+: _ :+: _ :+: _)

    def fileSystem[S[_]](implicit
      TS: Task :<: S,
      KVF: KVFile[S],
      KVR: KVRead[S],
      KVW: KVWrite[S],
      KVQ: KVQuery[S],
      MS: MonotonicSeq :<: S
    ): FileSystem ~> Free[S, ?] = interpretFileSystem(queryFile[S], readFile[S], writeFile[S], manageFile[S])

    def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): FileSystemDef[Free[S, ?]] = FileSystemDef fromPF {
      case FsCfg(FsType, uri) =>
        val defnTask: Task[DefinitionResult[Free[S, ?]]] = initialEffect(uri) map (run =>
          DefinitionResult(
            mapSNT(injectNT[Task, S] compose run) compose fileSystem,
            ().point[Free[S, ?]]
          )
        )
        lift(defnTask).into[S].liftM[DefErrT]
    }
  }
}
