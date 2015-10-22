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

import quasar.config._

import scalaz.Memo
import scalaz.concurrent.Task
import scalaz.syntax.apply._

import com.mongodb._

object util {
  private val DefaultOptions =
    (new MongoClientOptions.Builder)
      .serverSelectionTimeout(5000)
      .build

  private val mongoClient: ConnectionString => Task[MongoClient] = {
    val memo = Memo.mutableHashMapMemo[ConnectionString, MongoClient] { (uri: ConnectionString) =>
      new MongoClient(
        new MongoClientURI(uri.getConnectionString, new MongoClientOptions.Builder(DefaultOptions)))
    }

    uri => Task.delay { memo(uri) }
  }

  def createMongoClient(config: MongoDbConfig): Task[MongoClient] =
    disableMongoLogging *> mongoClient(config.uri)

  private def disableMongoLogging: Task[Unit] = {
    import java.util.logging._

    Task.delay { Logger.getLogger("org.mongodb").setLevel(Level.WARNING) }
  }
}
