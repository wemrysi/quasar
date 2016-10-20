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

package quasar.physical.jsonfile.fs

import quasar.Predef._
import quasar.fs._
import pathy.Path._
import scalaz._
import Scalaz.{ ToIdOps => _, _ }

class Tracer[S[_]](
  Q: QueryFile ~> Free[S, ?],
  R: ReadFile ~> Free[S, ?],
  W: WriteFile ~> Free[S, ?],
  M: ManageFile ~> Free[S, ?],
  traceCondition: String => Boolean
) {
  type FS[A] = Free[S, A]

  def fileSystem: FileSystem ~> Free[S, ?] = interpretFileSystem(queryFile, readFile, writeFile, manageFile)

  private def tracing[A: Show, B](label: String, value: A)(expr: => B): B = {
    if (traceCondition(label))
      println(label + ": " + value.show)

    expr
  }

  val queryFile = λ[QueryFile ~> FS] {
    case x @ QueryFile.ExecutePlan(lp, out) => tracing("Query.Execute", lp -> out)(Q(x))
    case x @ QueryFile.EvaluatePlan(lp)     => tracing("Query.Evaluate", lp)(Q(x))
    case x @ QueryFile.Explain(lp)          => tracing("Query.Explain", lp)(Q(x))
    case x @ QueryFile.More(rh)             => tracing("Query.More", rh)(Q(x))
    case x @ QueryFile.ListContents(dir)    => tracing("Query.List", dir)(Q(x))
    case x @ QueryFile.Close(fh)            => tracing("Query.Close", fh)(Q(x))
    case x @ QueryFile.FileExists(file)     => tracing("Query.Exists", file)(Q(x))
  }
  val readFile = λ[ReadFile ~> FS] {
    case x @ ReadFile.Open(file, offset, limit) => tracing("Read.Open", file)(R(x))
    case x @ ReadFile.Read(fh)                  => tracing("Read.Read", fh)(R(x))
    case x @ ReadFile.Close(fh)                 => tracing("Read.Close", fh)(R(x))
  }
  val writeFile = λ[WriteFile ~> FS] {
    case x @ WriteFile.Open(file)        => tracing("Write.Open", file)(W(x))
    case x @ WriteFile.Write(fh, chunks) => tracing("Write.Write", fh)(W(x))
    case x @ WriteFile.Close(fh)         => tracing("Write.Close", fh)(W(x))
  }
  val manageFile = λ[ManageFile ~> FS] {
    case x @ ManageFile.Move(scenario, semantics) => tracing("Manage.Move", scenario.toString -> semantics.toString)(M(x))
    case x @ ManageFile.Delete(path)              => tracing("Manage.Delete", path)(M(x))
    case x @ ManageFile.TempFile(path)            => tracing("Manage.TempFile", path)(M(x))
  }
}
object Tracer {
  val tracingProp: Option[String] = scala.sys.props get "ygg.trace" map (".*(" + _ + ").*")

  def maybe[S[_]](bfs: BoundFilesystem[S]): FileSystem ~> Free[S, ?] =
    tracingProp.fold(bfs.fileSystem)(re => bfs trace (_ matches re))
}

final case class BoundFilesystem[S[_]](
  Q: QueryFile ~> Free[S, ?],
  R: ReadFile ~> Free[S, ?],
  W: WriteFile ~> Free[S, ?],
  M: ManageFile ~> Free[S, ?]
) {
  def fileSystem: FileSystem ~> Free[S, ?]                  = interpretFileSystem(Q, R, W, M)
  def trace(p: String => Boolean): FileSystem ~> Free[S, ?] = new Tracer[S](Q, R, W, M, traceCondition = p).fileSystem
}

trait TransformAlgebras[S[_]] {
  import quasar.fp.free.flatMapSNT

  type SNT = S ~> Free[S, ?]

  def mapRead(implicit S: ReadFile :<: S): SNT
  def mapWrite(implicit S: WriteFile :<: S): SNT
  def mapManage(implicit S: ManageFile :<: S): SNT
  def mapQuery(implicit S: QueryFile :<: S): SNT

  def mapFs(implicit S0: ReadFile :<: S, S1: WriteFile :<: S, S2: ManageFile :<: S, S3: QueryFile :<: S): SNT = (
            flatMapSNT(mapRead)
    compose flatMapSNT(mapWrite)
    compose flatMapSNT(mapManage)
    compose mapQuery
  )
}
