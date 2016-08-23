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

package quasar.physical.marklogic.ejson

import quasar.Predef.{???, inline}
import quasar.ejson._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import matryoshka._
import scalaz.Coproduct
import scalaz.std.list._
import scalaz.syntax.std.boolean._
import simulacrum.typeclass

@typeclass
trait AsXQuery[F[_]] {
  def asXQuery: Algebra[F, XQuery]
}

object AsXQuery {
  implicit def coproductAsXQuery[F[_], G[_]](implicit F: AsXQuery[F], G: AsXQuery[G]): AsXQuery[Coproduct[F, G, ?]] =
    new AsXQuery[Coproduct[F, G, ?]] {
      val asXQuery: Algebra[Coproduct[F, G, ?], XQuery] =
        _.run.fold(F.asXQuery, G.asXQuery)
    }

  implicit val commonAsXQuery: AsXQuery[Common] =
    new AsXQuery[Common] {
      val asXQuery: Algebra[Common, XQuery] = {
        case Arr(xs) => mkSeq(xs)
        case Null()  => expr.emptySeq
        case Bool(b) => b.fold(fn.True, fn.False)
        case Str(s)  => s.xs
        case Dec(d)  => xs.decimal(d.toString.xs)
      }
    }

  implicit val objAsXQuery: AsXQuery[Obj] =
    new AsXQuery[Obj] {
      val asXQuery: Algebra[Obj, XQuery] =
        obj => map.new_(obj.value.toList.map { case (k, v) => map.entry(k.xs, v) })
    }

  implicit val extensionAsXQuery: AsXQuery[Extension] =
    new AsXQuery[Extension] {
      val asXQuery: Algebra[Extension, XQuery] = {
        case Meta(value, meta) => ???
        case Map(entries)      => ???
        case Byte(b)           => xs.byte(b.toInt.xqy)
        case Char(c)           => c.toString.xqy
        case Int(i)            => xs.integer(i.xqy)
      }
    }
}
