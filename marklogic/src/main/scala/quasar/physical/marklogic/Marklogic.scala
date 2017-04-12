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
package physical.marklogic

import slamdata.Predef._
import quasar.common._
import quasar.connector._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scala.Predef.implicitly

final class Marklogic(val config: (Positive, Positive)) extends BackendModule {
  type QS[T[_[_]]] = fs.queryfile.MLQScriptCP[T]
  type Config = (Positive, Positive)

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[Read[AFile], ?]]))

  // TODO
  type Repr = xquery.MainModule
  type M[A] = FileSystemErrT[PhaseResultT[fs.MLFSQ, ?], A]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def MonadFsErrM = MonadFsErr[M]
  def PhaseResultTellM = PhaseResultTell[M]
  def PhaseResultListenM = PhaseResultListen[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: OrderT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  def compile(uri: ConnectionUri): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    ???

  val Type = FileSystemType("marklogic")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT: OrderT](
      cp: T[QSM[T, ?]]): M[Repr] = ???

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): M[AFile] = ???
    def evaluatePlan(repr: Repr): M[ResultHandle] = ???
    def more(h: ResultHandle): M[Vector[Data]] = ???
    def close(h: ResultHandle): M[Unit] = ???
    def explain(repr: Repr): M[String] = ???
    def listContents(dir: ADir): M[Set[PathSegment]] = ???
    def fileExists(file: AFile): M[Boolean] = ???
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): M[ReadHandle] = ???
    def read(h: ReadHandle): M[Vector[Data]] = ???
    def close(h: ReadHandle): M[Unit] = ???
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    def open(file: AFile): M[WriteHandle] = ???
    def write(h: WriteHandle, chunk: Vector[Data]): M[Vector[FileSystemError]] = ???
    def close(h: WriteHandle): M[Unit] = ???
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): M[Unit] = ???
    def delete(path: APath): M[Unit] = ???
    def tempFile(near: APath): M[AFile] = ???
  }
}
