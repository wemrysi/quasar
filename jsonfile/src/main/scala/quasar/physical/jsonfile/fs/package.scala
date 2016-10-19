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

import quasar._
import quasar.Predef._
import quasar.fp._, free._, numeric._
import quasar.fs._
import quasar.fs.mount._, FileSystemDef._
import quasar.effect._
import quasar.fs.FileSystemError._
import quasar.Planner.UnsupportedPlan
import pathy.Path._
import quasar.contrib.pathy._
import scalaz._, Scalaz.{ ToIdOps => _, _ }
import scalaz.concurrent.Task
import FileSystemIndependentTypes._

// import ygg.table._
// import ygg.json.JValue
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
  val MoveSemantics  = ManageFile.MoveSemantics
  type FixPlan       = matryoshka.Fix[LogicalPlan]
  type MoveSemantics = ManageFile.MoveSemantics
  type Task[A]       = scalaz.concurrent.Task[A]

  val FsType = FileSystemType("jsonfile")

  val tracingProp: Option[String] = scala.sys.props get "ygg.trace" map (".*(" + _ + ").*")

  def isTracing(label: String): Boolean = tracingProp exists (label matches _)

  implicit class KVSOps[K, V, S[_]](val kvs: KVInject[K, V, S]) {
    type FS[A] = Free[S, A]

    implicit private def kvs_ = kvs
    private def Ops = KeyValueStore.Ops[K, V, S]

    def keys: FS[Vector[K]]               = Ops.keys
    def contains(key: K): FS[Boolean]     = Ops contains key
    def delete(key: K): FS[Boolean]       = for (exists <- contains(key) ; _ <- Ops delete key) yield exists
    def put(key: K, value: V): FS[Unit]   = Ops.put(key, value)
    def get(key: K): FS[Option[V]]        = (Ops get key).run
    def move(src: K, dst: K): FS[Boolean] = for (exists <- contains(src) ; _ <- Ops.move(src, dst)) yield exists
  }

  type FH = Vector[Data]
  type RH = ReadPos
  type WH = Vector[Data]
  type QH = Vector[Data]

  implicit def showPath: Show[APath]      = Show shows (posixCodec printPath _)
  implicit def showRHandle: Show[RHandle] = Show shows (r => "ReadHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showWHandle: Show[WHandle] = Show shows (r => "WriteHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showFixPlan: Show[FixPlan] = Show shows (lp => FPlan("", lp).toString)

  def tmpName(n: Long): String                  = s"__quasar.ygg$n"
  def unknownPath(p: APath): FileSystemError    = pathErr(PathError pathNotFound p)
  def unknownPlan(lp: FixPlan): FileSystemError = planningFailed(lp, UnsupportedPlan(lp.unFix, None))
  def makeDirList(names: PathSegment*): DirList = names.toSet

  def queryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]): QueryFile ~> Free[S, ?] = new FsAlgebras[S].queryFile
  def readFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]): ReadFile ~> Free[S, ?]    = new FsAlgebras[S].readFile
  def writeFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]): WriteFile ~> Free[S, ?] = new FsAlgebras[S].writeFile
  def manageFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]): ManageFile ~> Free[S, ?]                = new FsAlgebras[S].manageFile
}

package fs {
  final case class ReadPos(data: Chunks, offset: Int, limit: Int)

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
    ): FileSystem ~> Free[S, ?] = {
      val bfs = BoundFilesystem[S](queryFile[S], readFile[S], writeFile[S], manageFile[S])
      tracingProp.fold(bfs.fileSystem)(re => bfs trace (_ matches re))
    }

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
