/*
 * Copyright 2020 Precog Data
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

package quasar.impl.storage.mapdb

import slamdata.Predef._

import quasar.api.resource.{ResourcePath, ResourceName}

import scala.Predef.classOf
import scala.collection.mutable.ArrayBuilder

import java.util.Comparator

import org.mapdb.{DataInput2, DataOutput2, Serializer}
import org.mapdb.serializer.{GroupSerializer, SerializerArray}

object ResourcePathSerializer extends GroupSerializer[ResourcePath] {
  private val underlying = new SerializerArray(Serializer.STRING, classOf[String])

  def valueArrayCopyOfRange(vals: Any, from: Int, to: Int): AnyRef =
    underlying.valueArrayCopyOfRange(vals, from, to)

  def valueArrayDeleteValue(vals: Any, pos: Int): AnyRef =
    underlying.valueArrayDeleteValue(vals, pos)

  def valueArrayDeserialize(in: DataInput2, size: Int): AnyRef =
    underlying.valueArrayDeserialize(in, size)

  def valueArrayEmpty(): AnyRef =
    underlying.valueArrayEmpty()

  def valueArrayFromArray(objects: Array[AnyRef]): AnyRef =
    underlying.valueArrayFromArray(objects)

  def valueArrayGet(vals: Any, pos: Int): ResourcePath =
    fromArray(underlying.valueArrayGet(vals, pos))

  def valueArrayPut(vals: Any, pos: Int, newValue: ResourcePath): AnyRef =
    underlying.valueArrayPut(vals, pos, toArray(newValue))

  def valueArraySearch(keys: Any, key: ResourcePath, comparator: Comparator[_]): Int =
    underlying.valueArraySearch(keys, toArray(key), comparator)

  def valueArraySearch(keys: Any, key: ResourcePath): Int =
    underlying.valueArraySearch(keys, toArray(key))

  def valueArraySerialize(out: DataOutput2, vals: Any): Unit =
    underlying.valueArraySerialize(out, vals)

  def valueArraySize(vals: Any): Int =
    underlying.valueArraySize(vals)

  def valueArrayUpdateVal(vals: Any, pos: Int, newValue: ResourcePath): AnyRef =
    underlying.valueArrayUpdateVal(vals, pos, toArray(newValue))

  def deserialize(in: DataInput2, available: Int): ResourcePath =
    fromArray(underlying.deserialize(in, available))

  def serialize(out: DataOutput2, value: ResourcePath): Unit =
    underlying.serialize(out, toArray(value))

  override def equals(x: ResourcePath, y: ResourcePath): Boolean =
    if (x == null || y == null)
      x == y
    else
      underlying.equals(toArray(x), toArray(y))

  override def compare(x: ResourcePath, y: ResourcePath): Int =
    underlying.compare(toArray(x), toArray(y))

  ////

  private def fromArray(ss: Array[String]): ResourcePath =
    ss.foldLeft(ResourcePath.root())(_ / ResourceName(_))

  private def toArray(p: ResourcePath): Array[String] = {
    val b = ArrayBuilder.make[String]
    var unsnoc: Option[(ResourcePath, ResourceName)] = p.unsnoc

    while (unsnoc.isDefined) {
      b += unsnoc.get._2.value
      unsnoc = unsnoc.get._1.unsnoc
    }

    b.result.reverse
  }
}
