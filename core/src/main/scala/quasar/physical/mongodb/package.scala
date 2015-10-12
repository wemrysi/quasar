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

package quasar.physical

import quasar.namegen._
import quasar.jscore
import quasar.fs._

import scalaz._

import org.bson.Document
import com.mongodb.async.AsyncBatchCursor

package object mongodb {
  type BsonCursor          = AsyncBatchCursor[Document]
  type ReadState           = (Long, Map[ReadFile.ReadHandle, BsonCursor])
  type ReadStateT[F[_], A] = StateT[F, ReadState, A]
  type ReadMongo[A]        = ReadStateT[MongoDb, A]

  final case class NameGen(nameGen: Int)

  // used by State(T).runZero
  implicit val NameGenMonoid: Monoid[NameGen] = new Monoid[NameGen] {
    def zero = NameGen(0)
    def append(f1: NameGen, f2: => NameGen) = NameGen(f1.nameGen max f2.nameGen)
  }

  def freshId(label: String): State[NameGen, String] = for {
    n <- State((s: NameGen) => s.copy(nameGen = s.nameGen + 1) -> s.nameGen)
  } yield "__" + label + n.toString

  // TODO: parameterize over label (SD-512)
  def freshName: State[NameGen, BsonField.Name] =
    quasar.namegen.freshName("tmp").map(BsonField.Name)
}
