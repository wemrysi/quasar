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

package quasar

import quasar.Predef._
import matryoshka._, FunctorT.ops._, Recursive.ops._
import monocle.Prism
import scalaz._
import jawn._

package object ejson {
  def str[A] = Prism.partial[Common[A], String] { case Str(s) => s } (Str(_))

  /** For _strict_ JSON, you want something like `Obj[Mu[Json]]`.
    */
  type Json[A]  = Coproduct[Obj, Common, A]
  type EJson[A] = Coproduct[Extension, Common, A]

  val ExtEJson = implicitly[Extension :<: EJson]
  val CommonEJson = implicitly[Common :<: EJson]

  val fixParser    = jsonParser[Fix]
  val fixParserSeq = fixParser async AsyncParser.ValueStream

  def jawnParser[A](implicit z: Facade[A]): SupportParser[A] =
    new SupportParser[A] { implicit val facade: Facade[A] = z }

  def jawnParserSeq[A](implicit z: Facade[A]): AsyncParser[A] =
    jawnParser[A] async AsyncParser.ValueStream

  def jsonParser[T[_[_]]: Corecursive]: SupportParser[T[Json]] =
    jawnParser(jsonFacade[T, Json])

  def readJson[A: Facade](in: JsonInput): Try[A]            = JsonInput.readOneFrom(jawnParser[A], in)
  def readJsonSeq[A: Facade](in: JsonInput): Try[Vector[A]] = JsonInput.readSeqFrom(jawnParser[A], in)
  def readJsonFix(in: JsonInput): Try[Fix[Json]]            = JsonInput.readOneFrom(fixParser, in)
  def readJsonFixSeq(in: JsonInput): Try[Vector[Fix[Json]]] = JsonInput.readSeqFrom(fixParser, in)

  object EJson {
    def fromJson[A](f: String => A): Json[A] => EJson[A] =
      json => Coproduct(json.run.leftMap(Extension.fromObj(f)))

    def fromJsonT[T[_[_]]: FunctorT: Corecursive]: T[Json] => T[EJson] =
      _.transAna(fromJson(s => Coproduct.right[Obj](str[T[Json]](s)).embed))

    def fromCommon[T[_[_]]: Corecursive]: Common[T[EJson]] => T[EJson] =
      CommonEJson.inj(_).embed

    def fromExt[T[_[_]]: Corecursive]: Extension[T[EJson]] => T[EJson] =
      ExtEJson.inj(_).embed

    def isNull[T[_[_]]: Recursive](ej: T[EJson]): Boolean =
      CommonEJson.prj(ej.project).fold(false) {
        case ejson.Null() => true
        case _ => false
    }
  }
}
