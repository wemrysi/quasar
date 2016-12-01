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

package quasar.physical.fallback.fs

import scala.Predef.<:<
import quasar.Predef._
import scala.math.{ Ordering => SOrd }
import scala.collection.TraversableOnce

object RDD {
  implicit def rddToPairRDDFunctions[K: CTag, V: CTag](rdd: RDD[K->V]): RddPair[K, V] = new RddPair(rdd)
}

final case class RDD[A](xs: Vector[A]) {
  def ++(that: RDD[A]): RDD[A]                             = RDD(xs ++ that.xs)
  def collect()(implicit z: CTag[A]): Array[A]             = xs.toArray
  def filter(p: A => Boolean): RDD[A]                      = RDD(xs filter p)
  def first: A                                             = xs.head
  def flatMap[B: CTag](f: A => TraversableOnce[B]): RDD[B] = RDD(xs flatMap f)
  def foreach(f: A => Unit): Unit                          = xs foreach f
  def map[B](f: A => B): RDD[B]                            = RDD(xs map f)
  def sortBy[B: SOrd](f: A => B, order: Any): RDD[A]       = RDD(xs sortBy f)
  def toDebugString(): String                              = xs.toString
  def toVector(): Vector[A]                                = xs
  def zipWithIndex(): RDD[A -> Long]                       = RDD(xs.zipWithIndex map { case (k, v) => k -> v.toLong })

  def reduceByKey[K, V, R](f: (V, V) => V)(implicit ev: A <:< (K, V)): RDD[K -> V] = {
    // val buf = scala.collection.mutable.ListMap[K, V]()
    val buf = scala.collection.mutable.Map[K, V]()
    xs foreach { x =>
      val (k, v) = ev(x)

      ( if (buf contains k)
          buf(k) = f(buf(k), v)
        else
          buf(k) = v
      )
    }

    RDD(buf.toVector)
  }
}

final case class HashPartitioner(numPartitions: Int, f: Any => Int) extends Partitioner {
  def getPartition(key: Any): Int = f(key)
}

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 */
abstract class Partitioner {
  def numPartitions: Int
  def getPartition(key: Any): Int
}
object Partitioner {
  def apply(n: Int): Partitioner = new HashPartitioner(n, (x: Any) => x.## % n)
  val default = apply(1000)
}

class RddPair[K: CTag, V: CTag](self: RDD[K->V]) {
  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    join(other, 1000)
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = {
    join(other, Partitioner(numPartitions))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] =
    RDD(fullOuterJoin(other, partitioner).xs collect { case (k, (Some(l), Some(r))) => k -> ((l, r)) })

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, Partitioner.default)
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, Partitioner(numPartitions))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))] =
    RDD(fullOuterJoin(other, partitioner).xs collect { case (k, (Some(l), r)) => k -> ((l, r)) })

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, Partitioner.default)
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, Partitioner(numPartitions))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))] =
    RDD(fullOuterJoin(other, partitioner).xs collect { case (k, (l, Some(r))) => k -> ((l, r)) })

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD using the existing partitioner/
   * parallelism level.
   */
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = {
    fullOuterJoin(other, Partitioner.default)
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD into the given number of partitions.
   */
  def fullOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], Option[W]))] = {
    fullOuterJoin(other, Partitioner(numPartitions))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   */
  def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], Option[W]))] = {
    val lmap = self.xs.toMap
    val rmap = other.xs.toMap

    RDD((lmap.keySet ++ rmap.keySet).toVector map (k => k -> ((lmap get k, rmap get k))))
  }
}
