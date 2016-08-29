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

package ygg.pkg

import scala.collection.{ mutable => scm, immutable => sci }
import scalaz.Need

trait PackageAliases {
  // scala stdlib
  type ->[+A, +B]                     = scala.Tuple2[A, B]
  type =?>[-A, +B]                    = scala.PartialFunction[A, B]
  type Any                            = scala.Any
  type AnyRef                         = scala.AnyRef
  type ArrayBuffer[A]                 = scm.ArrayBuffer[A]
  type CBF[-From, -Elem, +To]         = scala.collection.generic.CanBuildFrom[From, Elem, To]
  type CTag[A]                        = scala.reflect.ClassTag[A]
  type ClassLoader                    = java.lang.ClassLoader
  type CoGroupResult[K, V, V1, CC[X]] = scSeq[K -> CoGroupValue[V, V1, CC]]
  type CoGroupValue[V, V1, CC[X]]     = scalaz.Either3[V, CC[V] -> CC[V1], V1]
  type Dynamic                        = scala.Dynamic
  type Either[+A, +B]                 = scala.util.Either[A, B]
  type Float                          = scala.Float
  type ListBuffer[A]                  = scm.ListBuffer[A]
  type PairOf[+A]                     = A -> A
  type Range                          = sci.Range
  type Regex                          = scala.util.matching.Regex
  type StringContext                  = scala.StringContext
  type Try[+A]                        = scala.util.Try[A]
  type Vec[+A]                        = scala.Vector[A]
  type jClass                         = java.lang.Class[_]
  type jConcurrentMap[K, V]           = java.util.concurrent.ConcurrentMap[K, V]
  type jPath                          = java.nio.file.Path
  type scIterable[A]                  = scala.collection.Iterable[A]
  type scIterator[+A]                 = scala.collection.Iterator[A]
  type scMap[K, V]                    = scala.collection.Map[K, V]
  type scSeq[A]                       = scala.collection.Seq[A]
  type scSet[A]                       = scala.collection.Set[A]
  type scTraversable[+A]              = scala.collection.Traversable[A]
  type sciMap[K, +V]                  = sci.Map[K, V]
  type sciQueue[+A]                   = sci.Queue[A]
  type sciSeq[+A]                     = sci.Seq[A]
  type sciTreeMap[K, +V]              = sci.TreeMap[K, V]
  type scmMap[K, V]                   = scm.Map[K, V]
  type scmPriorityQueue[A]            = scm.PriorityQueue[A]
  type scmSet[A]                      = scm.Set[A]
  type smOrdering[A]                  = scala.math.Ordering[A]
  type spec                           = scala.specialized
  type switch                         = scala.annotation.switch
  type transient                      = scala.transient
  type unchecked                      = scala.unchecked
  type volatile                       = scala.volatile
  val +:                              = scala.collection.+:
  val :+                              = scala.collection.:+
  val AnyRef                          = scala.AnyRef
  val ArrayBuffer                     = scm.ArrayBuffer
  val Boolean                         = scala.Boolean
  val Double                          = scala.Double
  val Float                           = scala.Float
  val Left                            = scala.util.Left
  val ListBuffer                      = scm.ListBuffer
  val Right                           = scala.util.Right
  val Seq                             = sci.Seq
  val Try                             = scala.util.Try
  val Vec                             = scala.Vector
  val scSeq                           = scala.collection.Seq
  val scmMap                          = scm.HashMap
  val scmPriorityQueue                = scm.PriorityQueue
  val scmSet                          = scm.HashSet

  // java stdlib
  type AtomicInt            = java.util.concurrent.atomic.AtomicInteger
  type AtomicLong           = java.util.concurrent.atomic.AtomicLong
  type BufferedOutputStream = java.io.BufferedOutputStream
  type BufferedReader       = java.io.BufferedReader
  type ByteBuffer           = java.nio.ByteBuffer
  type CharBuffer           = java.nio.CharBuffer
  type Charset              = java.nio.charset.Charset
  type Comparator[A]        = java.util.Comparator[A]
  type Exception            = java.lang.Exception
  type ExecutionContext     = java.util.concurrent.ExecutorService
  type File                 = java.io.File
  type FileInputStream      = java.io.FileInputStream
  type FileOutputStream     = java.io.FileOutputStream
  type IOException          = java.io.IOException
  type InputStream          = java.io.InputStream
  type InputStreamReader    = java.io.InputStreamReader
  type OutputStream         = java.io.OutputStream
  type OutputStreamWriter   = java.io.OutputStreamWriter
  type PrintStream          = java.io.PrintStream
  type Properties           = java.util.Properties
  type UUID                 = java.util.UUID
  type jMapEntry[K, V]      = java.util.Map.Entry[K, V]

  // other outside libs
  type Cmp               = scalaz.Ordering
  type Eq[A]             = scalaz.Equal[A]
  type LazyPairOf[+A]    = scalaz.Need[A -> A]
  type M[+A]             = scalaz.Need[A]
  type NeedEitherT[A, B] = scalaz.EitherT[Need, A, B]
  type NeedStreamT[A]    = scalaz.StreamT[Need, A]
  type Ord[A]            = scalaz.Order[A]
  val Eq                 = scalaz.Equal
  val Ord                = scalaz.Order
}
