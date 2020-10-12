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

package quasar.impl.storage.mvstore

import slamdata.Predef._

import java.nio.ByteBuffer
import java.lang.Object

import org.h2.mvstore.{DataUtils, WriteBuffer}
import org.h2.mvstore.`type`.{DataType, ObjectDataType}

private[impl] object ByteArrayDataType extends DataType {
  def compare(a: Object, b: Object): Int = {
    val arr = a.asInstanceOf[Array[Byte]]
    val brr = b.asInstanceOf[Array[Byte]]
    ObjectDataType.compareNotNull(arr, brr)
  }

  def getMemory(x: Object): Int =
    128 + 2 * x.asInstanceOf[Array[Byte]].length

  def read(buffer: ByteBuffer): Object = {
    val len = DataUtils.readVarInt(buffer)
    val tgt = new Array[Byte](len)
    val bytes = buffer.get(tgt, 0, len)
    bytes.asInstanceOf[Object]
  }

  def read(buffer: ByteBuffer, objs: Array[Object], len: Int, areKeys: Boolean): Unit = {
    var ix = 0
    while (ix < len) {
      read(buffer)
      ix += 1
    }
  }

  def write(buffer: WriteBuffer, obj: Object): Unit = {
    val arr = obj.asInstanceOf[Array[Byte]]
    buffer.putVarInt(arr.length).put(arr)
    ()
  }

  def write(buffer: WriteBuffer, objs: Array[Object], len: Int, areKeys: Boolean): Unit = {
    var ix = 0
    while (ix < len) {
      write(buffer, objs(ix))
      ix += 1
    }
  }
}
