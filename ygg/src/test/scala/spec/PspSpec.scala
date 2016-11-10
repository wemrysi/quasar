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

package ygg.tests

import ygg._, common._

class PspSpec extends TableQspec {
  val TD = fromJson(
    Seq(json"""{
      "Prices": [
        { "Product": "Potatoes", "Price": 3 },
        { "Product": "Avocadoes", "Price": 4 },
        { "Product": "Kiwis", "Price": 2 },
        { "Product": "Onions", "Price": 1 },
        { "Product": "Melons", "Price": 5 },
        { "Product": "Oranges", "Price": 5 },
        { "Product": "Tomatoes", "Price": 6 }
      ],
      "Quantities": [
        { "Product": "Potatoes", "Quantity": 45 },
        { "Product": "Avocadoes", "Quantity": 63 },
        { "Product": "Kiwis", "Quantity": 19 },
        { "Product": "Onions", "Quantity": 20 },
        { "Product": "Melons", "Quantity": 66 },
        { "Product": "Broccoli", "Quantity": 27 },
        { "Product": "Squash", "Quantity": 92 }
      ]
    }""")
  )

  // "in psp" >> {

  // }
}
