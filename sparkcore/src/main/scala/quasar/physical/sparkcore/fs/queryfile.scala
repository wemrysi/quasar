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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.{PhaseResults,PhaseResult, LogicalPlan, Data}
import quasar.qscript.QScriptTotal
import quasar.fs.QueryFile
import quasar.fs.QueryFile._
import quasar.fs._
import quasar.Planner._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fp.free._
import quasar.effect.Read

import java.io.File
import java.nio.file._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka._, Recursive.ops._
import pathy.Path._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

object queryfile {

  type SparkContextRead[A] = Read[SparkContext, A]

  def interperter[S[_]](implicit
    s0: Task :<: S,
    s1: SparkContextRead :<: S
  ): QueryFile ~> Free[S, ?] =
    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case FileExists(f) => fileExists(f)
        case ListContents(dir) => listContents(dir)
        case QueryFile.ExecutePlan(lp: Fix[LogicalPlan], out: AFile) => {
          (QueryFile.convertToQScript(lp).leftMap(planningFailed(lp, _)).traverse(executePlan(_, out, lp))).map(_.join.run.run)
          }
        case _ => ???
      }
    } 

  private def store(rdd: RDD[Data]): Task[AFile] = ???

  implicit def composedFunctor[F[_]: Functor, G[_]: Functor]:
      Functor[(F ∘ G)#λ] =
    new Functor[(F ∘ G)#λ] {
      def map[A, B](fa: F[G[A]])(f: A => B) = fa ∘ (_ ∘ f)
    }

  private def executePlan[S[_]](qs: Fix[QScriptTotal[Fix, ?]], out: AFile, lp: Fix[LogicalPlan])
    (implicit
      s0: Task :<: S,
      read: Read.Ops[SparkContext, S]
    ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, AFile]] = {

    val total = scala.Predef.implicitly[Planner.Aux[Fix, QScriptTotal[Fix, ?]]]

    read.asks { sc =>
      val sparkStuff: PlannerError \/ RDD[Data] =
        qs.cataM(total.plan).eval(sc)

      injectFT.apply {
        sparkStuff.bitraverse[(Task ∘ Writer[PhaseResults, ?])#λ, FileSystemError, AFile](
          planningFailed(lp, _).point[Writer[PhaseResults, ?]].point[Task],
          rdd => store(rdd).map (Writer(Vector(PhaseResult.Detail("RDD", rdd.toDebugString)), _))).map(EitherT(_))
      }
    }.join
  }

  private def fileExists[S[_]](f: AFile)(implicit
    s0: Task :<: S): Free[S, Boolean] =
    injectFT[Task, S].apply {
      Task.delay {
        Files.exists(Paths.get(posixCodec.unsafePrintPath(f)))
      }
    }

  private def listContents[S[_]](d: ADir)(implicit
    s0: Task :<: S): Free[S, FileSystemError \/ Set[PathSegment]] =
    injectFT[Task, S].apply {
      Task.delay {
        val directory = new File(posixCodec.unsafePrintPath(d))
        if(directory.exists()) {
          \/.fromTryCatchNonFatal{
            directory.listFiles.toSet[File].map {
              case file if file.isFile() => FileName(file.getName()).right[DirName]
              case directory => DirName(directory.getName()).left[FileName]
            }
          }
            .leftMap {
            case e =>
              pathErr(invalidPath(d, e.getMessage()))
          }
        } else pathErr(pathNotFound(d)).left[Set[PathSegment]]
      }
    }
}
