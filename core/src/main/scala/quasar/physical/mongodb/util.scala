/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{EnvErrF, EnvironmentError2}
import quasar.config._
import quasar.effect.Failure
import quasar.fp.free
import quasar.fp.prism._
import quasar.fs.mount.{ConnectionUri, FileSystemDef}

import java.util.concurrent.TimeUnit

import com.mongodb._
import com.mongodb.async.client.{MongoClient => AMongoClient, MongoClients, MongoClientSettings}
import com.mongodb.connection.ClusterSettings
import scalaz._
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

object util {
  import ConfigError._, EnvironmentError2._

  def createMongoClient(config: MongoDbConfig): Task[MongoClient] =
    disableMongoLogging *> mongoClient(config.uri)

  def createAsyncMongoClient[S[_]: Functor](
    uri: ConnectionUri
  )(implicit
    S0: Task :<: S,
    S1: EnvErrF :<: S,
    S2: CfgErrF :<: S
  ): Free[S, AMongoClient] = {
    type M[A] = Free[S, A]
    val cfgErr = Failure.Ops[ConfigError, S]
    val envErr = Failure.Ops[EnvironmentError2, S]

    val disableLogging =
      free.lift(disableMongoLogging).into[S]

    val connString =
      liftAndHandle(Task.delay(new ConnectionString(uri.value)))(t =>
        cfgErr.fail(malformedConfig(uri.value, t.getMessage)))

    def clientSettings(cs: ConnectionString) =
      MongoClientSettings.builder()
        .clusterSettings(
          DefaultClusterSettingsBuilder
            .applyConnectionString(cs)
            .build())
        .build()

    def createClient(settings: MongoClientSettings) =
      liftAndHandle(Task.delay(MongoClients.create(settings)))(t =>
        envErr.fail(connectionFailed(t.getMessage)))

    disableLogging *> connString map clientSettings >>= createClient
  }

  ////

  private val DefaultOptions =
    (new MongoClientOptions.Builder)
      .serverSelectionTimeout(5000)
      .build

  private def DefaultClusterSettingsBuilder =
    ClusterSettings.builder()
      .serverSelectionTimeout(5000, TimeUnit.MILLISECONDS)

  private val mongoClient: ConnectionString => Task[MongoClient] = {
    val memo = Memo.mutableHashMapMemo[ConnectionString, MongoClient] { (uri: ConnectionString) =>
      new MongoClient(
        new MongoClientURI(uri.getConnectionString, new MongoClientOptions.Builder(DefaultOptions)))
    }

    uri => Task.delay { memo(uri) }
  }

  private def disableMongoLogging: Task[Unit] = {
    import java.util.logging._
    Task.delay { Logger.getLogger("org.mongodb").setLevel(Level.WARNING) }
  }

  private def liftAndHandle[S[_]: Functor, A]
              (ta: Task[A])(f: Throwable => Free[S, A])
              (implicit S0: Task :<: S)
              : Free[S, A] =
    free.lift(ta.attempt).into[S].flatMap(_.fold(f, _.point[Free[S, ?]]))
}
