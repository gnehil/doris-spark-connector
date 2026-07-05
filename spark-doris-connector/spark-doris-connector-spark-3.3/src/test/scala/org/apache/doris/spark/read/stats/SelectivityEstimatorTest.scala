// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.read.stats

import org.apache.doris.spark.client.entity.DorisColumnStats

import org.apache.spark.sql.sources._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util

class SelectivityEstimatorTest {

  private def colStats(name: String, ndv: Long, nulls: Long, min: String, max: String): DorisColumnStats =
    new DorisColumnStats(name, ndv, nulls, 100L, 4.0d, min, max)

  private def buildMap(stats: DorisColumnStats*): util.Map[String, DorisColumnStats] = {
    val m = new util.HashMap[String, DorisColumnStats]()
    stats.foreach(s => m.put(s.getColumn, s))
    m
  }

  @Test
  def testEmptyFilters(): Unit = {
    val sel = SelectivityEstimator.estimate(Array.empty, new util.HashMap(), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testEqualTo(): Unit = {
    val cs = colStats("age", 100L, 100L, "1", "100")
    val sel = SelectivityEstimator.estimate(Array(EqualTo("age", 50)), buildMap(cs), 1000L)
    // nullRatio = 100/1000 = 0.1; sel = (1-0.1)/100 = 0.009
    assertEquals(0.009, sel, 0.001)
  }

  @Test
  def testIsNull(): Unit = {
    val cs = colStats("age", 100L, 200L, "1", "100")
    val sel = SelectivityEstimator.estimate(Array(IsNull("age")), buildMap(cs), 1000L)
    // nullRatio = 200/1000 = 0.2
    assertEquals(0.2, sel, 0.001)
  }

  @Test
  def testIsNotNull(): Unit = {
    val cs = colStats("age", 100L, 200L, "1", "100")
    val sel = SelectivityEstimator.estimate(Array(IsNotNull("age")), buildMap(cs), 1000L)
    assertEquals(0.8, sel, 0.001)
  }

  @Test
  def testIn(): Unit = {
    val cs = colStats("age", 100L, 0L, "1", "100")
    val sel = SelectivityEstimator.estimate(Array(In("age", Array(1, 2, 3))), buildMap(cs), 1000L)
    // sel = min(1, 3/100) * (1-0) = 0.03
    assertEquals(0.03, sel, 0.001)
  }

  @Test
  def testGreaterThan(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val sel = SelectivityEstimator.estimate(Array(GreaterThan("age", 25)), buildMap(cs), 1000L)
    // upper=true: ratio = (100-25)/100 = 0.75
    assertEquals(0.75, sel, 0.001)
  }

  @Test
  def testLessThan(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val sel = SelectivityEstimator.estimate(Array(LessThan("age", 25)), buildMap(cs), 1000L)
    // upper=false: ratio = (25-0)/100 = 0.25
    assertEquals(0.25, sel, 0.001)
  }

  @Test
  def testAnd(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val filters: Array[Filter] = Array(
      And(GreaterThan("age", 25), LessThan("age", 75))
    )
    val sel = SelectivityEstimator.estimate(filters, buildMap(cs), 1000L)
    // GT: (100-25)/100 = 0.75; LT: (75-0)/100 = 0.75; And: 0.75*0.75 = 0.5625
    assertEquals(0.5625, sel, 0.001)
  }

  @Test
  def testOr(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val filters: Array[Filter] = Array(
      Or(EqualTo("age", 1), EqualTo("age", 2))
    )
    val sel = SelectivityEstimator.estimate(filters, buildMap(cs), 1000L)
    // each: 1/100 = 0.01; Or: 0.01 + 0.01 - 0.01*0.01 = 0.0199
    assertEquals(0.0199, sel, 0.001)
  }

  @Test
  def testNot(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val sel = SelectivityEstimator.estimate(Array(Not(EqualTo("age", 50))), buildMap(cs), 1000L)
    // EqualTo: 1/100 = 0.01; Not: 1-0.01 = 0.99
    assertEquals(0.99, sel, 0.001)
  }

  @Test
  def testNotOfUnrecognisedPropagatesToOne(): Unit = {
    // StringContains is unrecognised → unknown → None.
    // Not(unknown) must also be unknown → 1.0 (NOT 0.0).
    val sel = SelectivityEstimator.estimate(Array(Not(StringContains("x", "y"))), new util.HashMap(), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testAndWithOneUnknownUsesOtherSide(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val filters: Array[Filter] = Array(
      And(EqualTo("age", 1), StringContains("name", "x"))
    )
    // EqualTo: 1/100 = 0.01; StringContains: unknown → None; And(0.01, None) = 0.01
    val sel = SelectivityEstimator.estimate(filters, buildMap(cs), 1000L)
    assertEquals(0.01, sel, 0.001)
  }

  @Test
  def testOrWithOneUnknownIsUnknown(): Unit = {
    val cs = colStats("age", 100L, 0L, "0", "100")
    val filters: Array[Filter] = Array(
      Or(EqualTo("age", 1), StringContains("name", "x"))
    )
    // Or(Some(0.01), None) = None → 1.0
    val sel = SelectivityEstimator.estimate(filters, buildMap(cs), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testUnrecognisedFilter(): Unit = {
    val sel = SelectivityEstimator.estimate(Array(StringContains("x", "y")), new util.HashMap(), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testMissingColumnStats(): Unit = {
    // filter on "age" but no stats for it → unknown → 1.0
    val sel = SelectivityEstimator.estimate(Array(EqualTo("age", 50)), new util.HashMap(), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testRangeNoMinMaxPropagatesUnknown(): Unit = {
    val cs = new DorisColumnStats("age", 100L, 0L, 100L, 4.0d, null, null)
    // Range with no min/max → None → unknown → 1.0
    val sel = SelectivityEstimator.estimate(Array(GreaterThan("age", 25)), buildMap(cs), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testNotOfRangeWithoutStatsIsOne(): Unit = {
    val cs = new DorisColumnStats("age", 100L, 0L, 100L, 4.0d, null, null)
    // Not(GT(unknown)) = Not(None) = None → 1.0 (not 0.0)
    val sel = SelectivityEstimator.estimate(Array(Not(GreaterThan("age", 25))), buildMap(cs), 1000L)
    assertEquals(1.0, sel, 0.001)
  }

  @Test
  def testRangeNonNumericMinPropagatesUnknown(): Unit = {
    val cs = colStats("name", 100L, 0L, "alice", "zoe")
    // min/max are strings → parseNumber fails → None → 1.0
    val sel = SelectivityEstimator.estimate(Array(GreaterThan("name", "m")), buildMap(cs), 1000L)
    assertEquals(1.0, sel, 0.001)
  }
}
