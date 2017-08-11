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

package quasar.physical.sparkcore.fs.local

import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.effect.Read
import quasar.fp.ski._
import quasar.physical.sparkcore.fs.readfile.Input

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import pathy.Path.posixCodec
import scalaz._
import scalaz.concurrent.Task

object readfile {

  def rddFrom[S[_]](f: AFile)
    (implicit read: Read.Ops[SparkContext, S]): Free[S, RDD[Data]] =
    read.asks { sc =>
      sc.textFile(posixCodec.unsafePrintPath(f))
        .map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
      }

  def input[S[_]](implicit read: Read.Ops[SparkContext, S], s0: Task :<: S) =
    Input(f => rddFrom(f))

}
