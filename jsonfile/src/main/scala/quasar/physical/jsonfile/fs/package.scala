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
import quasar.fp._, numeric._
import quasar.fs._
import pathy.Path, Path._
import quasar.contrib.pathy._
import scalaz._, Scalaz._
import FileSystemIndependentTypes._
import matryoshka._
import RenderTree.ops._

package object fs {
  val FsType = FileSystemType("jsonfile")

  type ADir         = quasar.contrib.pathy.ADir
  type AFile        = quasar.contrib.pathy.AFile
  type APath        = quasar.contrib.pathy.APath
  type AsTask[F[X]] = Task[F ~> Task]
  type EJson[A]     = quasar.ejson.EJson[A]
  type Fix[F[_]]    = matryoshka.Fix[F]
  type PathSegment  = quasar.contrib.pathy.PathSegment
  type Table        = ygg.table.Table
  type Task[A]      = scalaz.concurrent.Task[A]
  type Type         = quasar.Type

  val Task = scalaz.concurrent.Task
  val Type = quasar.Type

  def TODO: Nothing                                  = scala.Predef.???
  def cond[A](p: Boolean, ifp: => A, elsep: => A): A = if (p) ifp else elsep
  def diff[A: RenderTree](l: A, r: A): RenderedTree  = l.render diff r.render
  def showln[A: Show](x: A): Unit                    = println(x.shows)

  implicit class PathyRFPathOps(val path: Path[Any, Any, Sandboxed]) {
    def toAbsolute: APath = mkAbsolute(rootDir, path)
    def toJavaFile: jFile = new jFile(posixCodec unsafePrintPath path)
  }

  implicit def showPath: Show[APath]      = Show shows (posixCodec printPath _)
  implicit def showRHandle: Show[RHandle] = Show shows (r => "ReadHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showWHandle: Show[WHandle] = Show shows (r => "WriteHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showFixPlan: Show[FixPlan] = Show shows (lp => FPlan("", lp).toString)
}
