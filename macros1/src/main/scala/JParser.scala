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

package ygg.macros

import quasar.Predef._
import jawn._, AsyncParser._
import scala.util.{ Success, Failure }

object JParser {
  type ResultSeq[A] = Either[ParseException, scala.collection.Seq[A]]
  type ResultVec[A] = Either[ParseException, Vec[A]]

  def stream[A](implicit z: Facade[A]): AsyncParser[A] = Parser.async[A](ValueStream)
  def json[A](implicit z: Facade[A]): AsyncParser[A]   = Parser.async[A](SingleValue)
  def unwrap[A](implicit z: Facade[A]): AsyncParser[A] = Parser.async[A](UnwrapArray)

  def parseUnsafe[A](str: String)(implicit z: Facade[A]): A = Parser.parseUnsafe[A](str)

  def parse[A](str: String)(implicit z: Facade[A]): Try[A]          = Try(parseUnsafe[A](str))
  def parseMany[A](str: String)(implicit z: Facade[A]): Try[Vec[A]] = exhaust(stream[A])(_ absorb str)

  private def exhaust[A](p: AsyncParser[A])(f: AsyncParser[A] => ResultSeq[A])(implicit z: Facade[A]): Try[Vec[A]] =
    ( for (r1 <- f(p).right ; r2 <- p.finish.right) yield r1 ++ r2 ) match {
      case Left(t)  => Failure(t)
      case Right(x) => Success(x.toVector)
    }
}
