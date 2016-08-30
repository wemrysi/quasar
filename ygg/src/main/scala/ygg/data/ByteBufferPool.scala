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

package ygg.data

import ygg._, common._
import scalaz._

import java.nio.ByteBuffer
import java.util.concurrent.{ BlockingQueue, ArrayBlockingQueue, LinkedBlockingQueue }
import java.util.concurrent.atomic.AtomicLong
import java.lang.ref.SoftReference
import ByteBufferPool._

/**
  * A `Monad` for working with `ByteBuffer`s.
  */
trait ByteBufferMonad[M[_]] extends Monad[M] {
  def getBuffer(min: Int): M[ByteBuffer]
}

final class ByteBufferPool(val capacity: Int) {
  private val fixedBufferCount = 8
  private val _hits            = new AtomicLong()
  private val _misses          = new AtomicLong()

  val fixedBufferQueue: BlockingQueue[ByteBuffer]               = new ArrayBlockingQueue(fixedBufferCount)
  val flexBufferQueue: BlockingQueue[SoftReference[ByteBuffer]] = new LinkedBlockingQueue()

  def hits   = _hits.get()
  def misses = _hits.get()

  /**
    * Returns a cleared `ByteBuffer` that can store `capacity` bytes.
    */
  def acquire: ByteBuffer = {
    var buffer = fixedBufferQueue.poll()

    if (buffer == null) {
      var ref = flexBufferQueue.poll()
      buffer = if (ref != null) ref.get else null
      while (ref != null && buffer == null) {
        ref = flexBufferQueue.poll()
        buffer = if (ref != null) ref.get else null
      }
    }

    if (buffer == null) {
      _misses.incrementAndGet()
      buffer = ByteBuffer.allocate(capacity)
    } else {
      _hits.incrementAndGet()
    }

    buffer.clear()
    buffer
  }

  /**
    * Releases a `ByteBuffer` back into the pool for re-use later on.
    */
  def release(buffer: ByteBuffer): Unit = {
    if (!(fixedBufferQueue offer buffer))
      discard(flexBufferQueue offer (new SoftReference(buffer)))
  }

  def toStream: Stream[ByteBuffer] = Stream.continually(acquire)

  def run[A](a: Pool[A]): A = a.eval(PoolData(this, Vec())).run
}

object ByteBufferPool {
  import Free._

  final case class PoolData(pool: ByteBufferPool, buffers: Vec[ByteBuffer]) {
    def _1 = pool
    def _2 = buffers
  }
  type Pool[A] = StateT[Trampoline, PoolData, A]

  implicit object PoolMonad extends ByteBufferMonad[Pool] {
    // equivalent to: StateT[Trampoline, PoolData, A](s => Free.pure((s, a)))
    def point[A](a: => A): Pool[A]                        = StateT stateT a
    def bind[A, B](fa: Pool[A])(f: A => Pool[B]): Pool[B] = StateT(s =>
      s match {
        case PoolData(p, bs) => ???
      }
    )

    def getBuffer(min: Int): Pool[ByteBuffer] = acquire(min)
  }

  def apply(): ByteBufferPool = new ByteBufferPool(16 * 1024)

  /**
    * Acquire a `ByteBuffer` and add it to the state.
    */
  def acquire(min: Int): Pool[ByteBuffer] = StateT {
    case PoolData(pool, buffers @ (buf +: _)) if buf.remaining() >= min => pure(PoolData(pool, buffers) -> buf)
    case PoolData(pool, buffers)                                        => pure(pool.acquire |> (buf => PoolData(pool, buf +: buffers) -> buf))
  }

  def acquire: Pool[ByteBuffer] = acquire(512)

  /**
    * Reverses the state (list of `ByteBuffer`s) and returns an `Array[Byte]` of
    * the contiguous bytes in all the buffers.
    */
  def flipBytes: Pool[Array[Byte]] = StateT {
    case PoolData(pool, revBufs) => pure(revBufs.reverse |> (bufs => PoolData(pool, bufs) -> getBytesFrom(bufs)))
  }

  /**
    * Removes and releases all `ByteBuffer`s in the state to the pool.
    */
  def release: Pool[Unit] = StateT {
    case pd @ PoolData(pool, buffers) =>
      buffers foreach (pool release _)
      pure(pd -> (()))
  }

  def getBytesFrom(buffers: Vec[ByteBuffer]): Array[Byte] = {
    val size = buffers.foldLeft(0) { (size, buf) =>
      buf.flip()
      size + buf.remaining()
    }
    val bytes = new Array[Byte](size)
    buffers.foldLeft(0) { (offset, buf) =>
      val length = buf.remaining()
      buf.get(bytes, offset, length)
      offset + length
    }
    bytes
  }
}
