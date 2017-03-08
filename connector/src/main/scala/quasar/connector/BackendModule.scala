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

import quasar.Predef._
import quasar.common.PhaseResults
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.numeric.{Natural, Positive}
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs._
import quasar.fs.mount._

import matryoshka._
import scalaz._
import scalaz.concurrent.Task

trait BackendModule {
  type QS[T[_[_]]] <: CoM

  // TODO need to provide implicit materialization from QScriptRead[T, A] to QS[T]#M[A]
  // it's tempting to just have the implementor provide information about the QS
  // structure so that we can factor out the boilerplate (the various colaesce calls, etc),
  // but that's tricky because the Marklogic connector has some specific WAYS in which
  // it wants us to transform things, specifically because of the Read/ShiftedRead
  // madness.  so we may have to materialize some more specific things here

  type Config

  type Repr
  type M[A]

  implicit def M: Monad[M]

  def compile: M ~> Task

  final def definition(config: Config): FileSystemDef[Task] = ???

  final def lpToRepr[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](lp: T[LogicalPlan]): M[Repr] = {
    // TODO do magic things with typeclasses!  yay! coalesce stuff; optimize; normalize; thingies
    ???
  }

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: QS[T]): M[Repr]

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
