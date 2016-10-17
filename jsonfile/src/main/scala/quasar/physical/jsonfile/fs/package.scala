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
import ygg.table.Table
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

package object fs {
  val FsType = FileSystemType("jsonfile")

  implicit def showPath: Show[APath]      = Show shows (posixCodec printPath _)
  implicit def showRHandle: Show[RHandle] = Show shows (r => "ReadHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showWHandle: Show[WHandle] = Show shows (r => "WriteHandle(%s, %s)".format(r.file.show, r.id))

  def kvEmpty[K, V] = KeyValueStore.impl.empty[K, V]

  type KVFiles[A] = KeyValueStore[AFile, Table, A]
  type KVRead[A]  = KeyValueStore[RHandle, Table, A]

  type JsonFs[A] = (
        Task
    :\: KVFiles
    :\: KVRead
    :/: MonotonicSeq
  )#M[A]

  def jsonFs(uri: ConnectionUri): AsTask[JsonFs] = (
        kvEmpty[AFile, Table]
    |@| kvEmpty[RHandle, Table]
    |@| MonotonicSeq.fromZero
  )(reflNT[Task] :+: _ :+: _ :+: _)

  type AsTask[M[X]] = Task[M ~> Task]

  def fileSystem[S[_]](implicit
    T: Task :<: S,
    KVF: KVFiles :<: S,
    KVR: KVRead :<: S,
    MS: MonotonicSeq :<: S
  ): FileSystem ~> Free[S, ?] = interpretFileSystem(queryFile[S], readFile[S], writeFile[S], manageFile[S])

  def definition[S[_]](implicit S0: Task :<: S): FileSystemDef[Free[S, ?]] = FileSystemDef fromPF {
    case FsCfg(FsType, uri) =>
      val defnTask: Task[DefinitionResult[Free[S, ?]]] = jsonFs(uri) map (run =>
        DefinitionResult(
          mapSNT(injectNT[Task, S] compose run) compose fileSystem,
          ().point[Free[S, ?]]
        )
      )

      lift(defnTask).into[S].liftM[DefErrT]
  }

  def tmpName(n: Long): String                  = s"__quasar.jsonfile_$n"
  def unknownPath(p: APath): FileSystemError    = pathErr(PathError pathNotFound p)
  def unknownPlan(lp: FixPlan): FileSystemError = planningFailed(lp, UnsupportedPlan(lp.unFix, None))
  def makeDirList(names: PathSegment*): DirList = names.toSet

  def nextUid[S[_]](implicit MS: MonotonicSeq :<: S) = MonotonicSeq.Ops[S].next

  implicit class KVSOps[K, V, S[_]](val kvs: KeyValueStore[K, V, ?] :<: S) {
    implicit def kvs_ = kvs

    val Ops = KeyValueStore.Ops[K, V, S]

    def keys             = Ops.keys
    def contains(key: K) = Ops contains key
    def delete(key: K)   = Ops delete key
  }

  def listFiles[S[_]](implicit KVF: KVFiles :<: S)              = KeyValueStore.Ops[AFile, Table, S].keys
  def containsFile[S[_]](f: AFile)(implicit KVF: KVFiles :<: S) = KeyValueStore.Ops[AFile, Table, S].contains(f)

  def queryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFiles :<: S): QueryFileSystem[Free[S, ?]] = new QueryFileSystem[Free[S, ?]] with STypes[S] {
    def closeQ(rh: QHandle): FSUnit                   = ()
    def evaluate(lp: FixPlan): FPLR[QHandle]          = (phaseResults(lp) |@| nextUid)((ph, uid) => ph -> \/-(QHandle(uid)))
    def execute(lp: FixPlan, out: AFile): FPLR[AFile] = phaseResults(lp) tuple \/-(out)
    def explain(lp: FixPlan): FPLR[ExecutionPlan]     = phaseResults(lp) tuple ExecutionPlan(FsType, "...")

    private def phaseResults(lp: FixPlan): FS[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", "<no description>"))

    def exists(file: AFile): FSBool    = KVF contains file
    def list(dir: ADir): FLR[DirList]  = listFiles[S] map (_ flatMap pathName) map (xs => makeDirList(xs: _*).right)
    def more(rh: QHandle): FLR[Chunks] = Vector()
  }

  def trace[A: Show](msg: String)(value: A): A = { println(msg + ": " + value.show) ; value }

  def readFile[S[_]](implicit MS: MonotonicSeq :<: S): ReadFileSystem[Free[S, ?]] = new ReadFileSystem[Free[S, ?]] with STypes[S] {
    def openForRead(file: AFile, offset: Natural, limit: Option[Positive]): FLR[RHandle] = nextUid map (uid => trace("openForRead")(RHandle(file, uid)).right)
    def read(fh: RHandle): FLR[Chunks]                                                   = Vector()
    def closeR(fh: RHandle): FSUnit                                                      = ()
  }

  def writeFile[S[_]](implicit MS: MonotonicSeq :<: S): WriteFileSystem[Free[S, ?]] = new WriteFileSystem[Free[S, ?]] with STypes[S] {
    def openForWrite(file: AFile): FLR[WHandle]        = nextUid map (uid => trace("openForWrite")(WHandle(file, uid)).right)
    def write(fh: WHandle, chunks: Chunks): FS[Errors] = Vector()
    def closeW(fh: WHandle): FSUnit                    = ()
  }

  def manageFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFiles :<: S): ManageFileSystem[Free[S, ?]] = new ManageFileSystem[Free[S, ?]] with STypes[S] {
    def createTempFile(near: APath): FLR[AFile] =
      MonotonicSeq.Ops[S].next map { i =>
        val name = file(tmpName(i))
        refineType(near).fold(
          _ </> name,
          f => fileParent(f) </> name
        ).right
      }

    def deletePath(path: APath): FLR[Unit]                                    = refineType(path).fold(x => (), x => ignore(KVF delete trace("deletePath")(x)))
    def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]    = ()
    def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics): FLR[Unit] = ()
  }
}

package fs {
  trait STypes[S[_]] extends EitherTContextLeft[Free[S, ?], FileSystemError] {
    implicit protected val applicative: Applicative[FS] = scalaz.Free.freeMonad[S]

    type FS[A]  = Free[S, A]
    type FSUnit = FS[Unit]
    type FSBool = FS[Boolean]
  }
}
