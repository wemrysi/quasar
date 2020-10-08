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
import java.lang.{Byte => JByte, Object}

import org.h2.mvstore.WriteBuffer
import org.h2.mvstore.`type`.DataType

private[mvstore] object ByteArrayDataType extends DataType {
  def compare(a: Object, b: Object): Int = {
    val arr = a.asInstanceOf[Array[Byte]]
    val brr = b.asInstanceOf[Array[Byte]]
    val alen = arr.length
    val blen = brr.length
    compareImpl(arr, brr, alen, blen, 0)
  }

  @scala.annotation.tailrec
  private def compareImpl(as: Array[Byte], bs: Array[Byte], alen: Int, blen: Int, ix: Int): Int =
    if (ix >= alen) {
      if (ix >= blen) 0 else -1
    } else {
      if (ix >= blen) 1
      else {
        val a = as(ix)
        val b = bs(ix)
        val compared = JByte.compare(a, b)
        if (compared != 0) compared
        else compareImpl(as, bs, alen, blen, ix + 1)
      }
    }

  def getMemory(x: Object): Int =
    x.asInstanceOf[Array[Byte]].length

  def read(buffer: ByteBuffer): Object = {
    val len = buffer.getInt()
    val tgt = new Array[Byte](len)
    val bytes = buffer.get(tgt, 0, len)
    bytes.asInstanceOf[Object]
  }

  def read(buffer: ByteBuffer, objs: Array[Object], len: Int, areKeys: Boolean): Unit =
    for (i <- 0 until len) objs(i) = read(buffer)

  def write(buffer: WriteBuffer, obj: Object): Unit = {
    val arr = obj.asInstanceOf[Array[Byte]]
    buffer.putInt(arr.length).put(arr)
    ()
  }
  def write(buffer: WriteBuffer, objs: Array[Object], len: Int, areKeys: Boolean): Unit =
    for (i <- 0 until len) write(buffer, objs(i))
}
