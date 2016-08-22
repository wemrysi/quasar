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

package quasar.physical.sparkcore

import quasar.Predef._
import quasar.fs._
import quasar.fs.mount.ConnectionUri

import pathy.Path._
import org.apache.spark._

package object fs {

  final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  def parseUri(uri: ConnectionUri): Option[SparkFSConf] = {

    def forge(master: String, rootPath: String): Option[SparkFSConf] =
      posixCodec.parseAbsDir(rootPath).map { prefix =>
        SparkFSConf(new SparkConf().setMaster(master), sandboxAbs(prefix))
      }
    
    uri.value.split('|').toList match {
      case master :: prefixPath :: Nil => forge(master, prefixPath)
      case a => None
    }
  }
}
