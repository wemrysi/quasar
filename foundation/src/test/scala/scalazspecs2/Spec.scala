/*
 * Copyright 2014–2017 SlamData Inc.
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

package org.specs2.scalaz

import scala.language.implicitConversions

import org.specs2._
import org.specs2.matcher._
import org.specs2.specification.core._
import org.specs2.scalacheck._
import org.specs2.main.{ArgumentsShortcuts, ArgumentsArgs}

import org.scalacheck.{Prop, Properties}
import org.scalacheck.util.{FreqMap, Pretty}
import scala.Predef._
import scala._

/** A minimal version of the Specs2 mutable base class */
trait Spec extends org.specs2.mutable.Spec with ScalaCheck
  with MatchersImplicits with StandardMatchResults
  with ArgumentsShortcuts with ArgumentsArgs {
    
  val ff = fragmentFactory; import ff._
  
  setArguments(fullStackTrace)

  def checkAll(name: String, props: Properties)(implicit p: Parameters, f: FreqMap[Set[Any]] => Pretty): Fragments = {
    addFragment(text(s"$name  ${props.name} must satisfy"))
    addFragments(Fragments.foreach(props.properties) { case (name, prop) => Fragments(name in check(prop, p, f)) })
  }

  def checkAll(props: Properties)(implicit p: Parameters, f: FreqMap[Set[Any]] => Pretty): Fragments = {
    addFragment(text(s"${props.name} must satisfy"))
    addFragments(Fragments.foreach(props.properties) { case (name, prop) => Fragments(name in check(prop, p, f)) })
  }

  implicit def enrichProperties(props: Properties) = new {
    def withProp(propName: String, prop: Prop) = new Properties(props.name) {
      for {(name, p) <- props.properties} property(name) = p
      property(propName) = prop
    }
  }

}
