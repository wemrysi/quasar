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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.connector.EnvironmentError
import quasar.effect._
import quasar.fp.free._
import quasar.fp.TaskRef
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, BackendDef._
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile, readfile => corereadfile}

import scala.sys

import org.http4s.{ParseFailure, Uri}
import org.apache.spark._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object cassandra {

  import corequeryfile.RddState

  val FsType = FileSystemType("spark-cassandra")

  type Eff7[A] = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Read[SparkContext, ?], A]
  type Eff6[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Eff7, A]
  type Eff5[A] = Coproduct[KeyValueStore[WriteHandle, AFile, ?], Eff6, A]
  type Eff4[A] = Coproduct[Task, Eff5, A]
  type Eff3[A] = Coproduct[PhysErr, Eff4, A]
  type Eff2[A] = Coproduct[CassandraDDL, Eff3, A]
  type Eff1[A] = Coproduct[MonotonicSeq, Eff2, A]
  type Eff[A]  = Coproduct[SparkConnectorDetails, Eff1, A]

  final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  val parseUri: ConnectionUri => Task[DefinitionError \/ (SparkConf, SparkFSConf)] = (connUri: ConnectionUri) => Task.delay {

    def liftErr(msg: String): DefinitionError = NonEmptyList(msg).left[EnvironmentError]

    def master(host: String, port: Int): State[SparkConf, Unit] =
      State.modify(_.setMaster(s"spark://$host:$port"))
    def appName: State[SparkConf, Unit] = State.modify(_.setAppName("quasar"))
       def config(name: String, uri: Uri): State[SparkConf, Unit] =
      State.modify(c => uri.params.get(name).fold(c)(c.set(name, _)))
    val uriOrErr: DefinitionError \/ Uri = Uri.fromString(connUri.value).leftMap((pf: ParseFailure) => liftErr(pf.toString))

    val sparkConfOrErr: DefinitionError \/ SparkConf = for {
      uri <- uriOrErr
      host <- uri.host.fold(NonEmptyList("host not provided").left[EnvironmentError].left[Uri.Host])(_.right[DefinitionError])
      port <- uri.port.fold(NonEmptyList("port not provided").left[EnvironmentError].left[Int])(_.right[DefinitionError])
    } yield {

      ( master(host.value, port)                       *>
        appName                                        *>
        config("spark.executor.memory", uri)           *>
        config("spark.executor.cores", uri)            *>
        config("spark.executor.extraJavaOptions", uri) *>
        config("spark.default.parallelism", uri)       *>
        config("spark.files.maxPartitionBytes", uri)   *>
        config("spark.driver.cores", uri)              *>
        config("spark.driver.maxResultSize", uri)      *>
        config("spark.driver.memory", uri)             *>
        config("spark.local.dir", uri)                 *>
        config("spark.reducer.maxSizeInFlight", uri)   *>
        config("spark.reducer.maxReqsInFlight", uri)   *>
        config("spark.shuffle.file.buffer", uri)       *>
        config("spark.shuffle.io.retryWait", uri)      *>
        config("spark.memory.fraction", uri)           *>
        config("spark.memory.storageFraction", uri)    *>
        config("spark.cores.max", uri)                 *>
        config("spark.speculation", uri)               *>
        config("spark.task.cpus", uri)
      ).exec(new SparkConf())
    }

    val rootPathOrErr: DefinitionError \/ ADir =
      uriOrErr
        .flatMap(uri =>
          uri.params.get("rootPath").fold(liftErr("'rootPath' parameter not provided").left[String])(_.right[DefinitionError])
        )
        .flatMap(pathStr =>
          posixCodec.parseAbsDir(pathStr)
            .map(unsafeSandboxAbs)
            .fold(liftErr("'rootPath' is not a valid path").left[ADir])(_.right[DefinitionError])
        )

    def fetchParameter(name: String): DefinitionError \/ String = uriOrErr.flatMap(uri =>
      uri.params.get(name).fold(liftErr(s"'$name' parameter not provided").left[String])(_.right[DefinitionError])
    )

    for {
      initSparkConf          <- sparkConfOrErr
      host                  <- fetchParameter("cassandraHost")
      port                  <- fetchParameter("cassandraPort")
      rootPath              <- rootPathOrErr
    } yield {
      val sparkConf = initSparkConf.set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.port", port)
      (sparkConf, SparkFSConf(sparkConf, rootPath))
    }
  }

  private def fetchSparkCoreJar: Task[String] = Task.delay {
    sys.env("QUASAR_HOME") + "/sparkcore.jar"
  }

  private def sparkFsDef[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): SparkFSConf => Free[S, SparkFSDef[Eff, S]] = (sfsc: SparkFSConf) => {

    val genSc = fetchSparkCoreJar.map { jar => 
      val sc = new SparkContext(sfsc.sparkConf)
      sc.addJar(jar)
      sc
    }

    lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, RddState]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, AFile]) |@|
      genSc) {
      // TODO better names!
      (genState, rddStates, sparkCursors, writehandlers, sc) => {

        type Temp[A] = Coproduct[CassandraDDL, Task, A]
        def temp: Free[Temp, ?] ~> Task =
          foldMapNT(CassandraDDL.interpreter[S](sc) :+: injectNT[Task, Task])

        val interpreter: Eff ~> S =
          (queryfile.detailsInterpreter[Temp] andThen temp  andThen injectNT[Task, S]) :+:
        (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
        (CassandraDDL.interpreter[S](sc) andThen injectNT[Task, S]) :+:
        injectNT[PhysErr, S] :+:
        injectNT[Task, S]  :+:
        (KeyValueStore.impl.fromTaskRef[WriteHandle, AFile](writehandlers) andThen injectNT[Task, S]) :+:
        (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
        (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
        (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

        SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
      }
    }).into[S]
  }

  private def fsInterpret: SparkFSConf => (FileSystem ~> Free[Eff, ?]) =
    (sparkFsConf: SparkFSConf) => interpretFileSystem(
      corequeryfile.chrooted[Eff](FsType, sparkFsConf.prefix),
      corereadfile.chrooted(sparkFsConf.prefix),
      writefile.chrooted[Eff](sparkFsConf.prefix),
      managefile.chrooted[Eff](sparkFsConf.prefix))

  def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): BackendDef[Free[S, ?]] =
    quasar.physical.sparkcore.fs.definition[Eff, S, SparkFSConf](FsType, parseUri, sparkFsDef, fsInterpret)

}
