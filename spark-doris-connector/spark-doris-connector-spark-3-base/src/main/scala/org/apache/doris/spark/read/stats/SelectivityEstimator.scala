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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._

import scala.util.Try

/**
 * Estimates filter selectivity using column statistics fetched from Doris.
 *
 * <p>The estimator operates on V1 [[Filter]] objects so that the same logic
 * works across Spark 3.1 - 3.5. Version-specific scan classes are responsible
 * for converting V2 predicates to V1 filters when needed.
 *
 * <p>Rules (simplified, independence assumption):
 * <ul>
 *   <li>EqualTo → (1 - nullRatio) / ndv</li>
 *   <li>IsNull → nullRatio; IsNotNull → 1 - nullRatio</li>
 *   <li>In(col, vs) → min(1, |vs| / ndv) * (1 - nullRatio)</li>
 *   <li>Range (GT/LT/GE/LE) → linear interpolation on min/max for numeric types only</li>
 *   <li>And → a * b; Or → a + b - a*b; Not → 1 - sel</li>
 *   <li>Unrecognised → 1.0 (conservative), and this "unknown" propagates:
 *       And(unknown, x) = x, Or(unknown, x) = 1.0, Not(unknown) = 1.0</li>
 * </ul>
 */
private[spark] object SelectivityEstimator extends Logging {

  /**
   * @param filters     pushed filters (already accepted by Doris)
   * @param colStats    column-level stats keyed by column name
   * @param rowCount     total row count (used for null ratio)
   * @return combined selectivity in [0, 1]
   */
  def estimate(
      filters: Array[Filter],
      colStats: java.util.Map[String, DorisColumnStats],
      rowCount: Long): Double = {
    if (filters == null || filters.isEmpty) return 1.0
    var selOpt: Option[Double] = Some(1.0)
    for (f <- filters) {
      selOpt = andCombine(selOpt, estimateFilter(f, colStats, rowCount))
    }
    selOpt.getOrElse(1.0)
  }

  /**
   * Estimate a single filter. Returns None when the filter (or any sub-filter)
   * is unrecognised — None is the "unknown" sentinel that propagates conservatively.
   */
  private def estimateFilter(f: Filter, colStats: java.util.Map[String, DorisColumnStats], rowCount: Long): Option[Double] = f match {
    case EqualTo(attr, _) => withStats(attr, colStats, rowCount) { cs =>
      val nullRatio = nullRatioOf(cs, rowCount)
      val ndv = math.max(cs.getNdv, 1L)
      Some((1.0 - nullRatio) / ndv)
    }
    case EqualNullSafe(attr, _) => withStats(attr, colStats, rowCount) { cs =>
      val ndv = math.max(cs.getNdv, 1L)
      Some(1.0 / ndv)
    }
    case IsNull(attr) => withStats(attr, colStats, rowCount) { cs =>
      Some(nullRatioOf(cs, rowCount))
    }
    case IsNotNull(attr) => withStats(attr, colStats, rowCount) { cs =>
      Some(1.0 - nullRatioOf(cs, rowCount))
    }
    case In(attr, values) if values.nonEmpty => withStats(attr, colStats, rowCount) { cs =>
      val nullRatio = nullRatioOf(cs, rowCount)
      val ndv = math.max(cs.getNdv, 1L)
      Some(math.min(1.0, values.length.toDouble / ndv) * (1.0 - nullRatio))
    }
    case GreaterThan(attr, v) => rangeSelectivity(attr, v, colStats, upper = true)
    case GreaterThanOrEqual(attr, v) => rangeSelectivity(attr, v, colStats, upper = true)
    case LessThan(attr, v) => rangeSelectivity(attr, v, colStats, upper = false)
    case LessThanOrEqual(attr, v) => rangeSelectivity(attr, v, colStats, upper = false)
    case And(left, right) => andCombine(estimateFilter(left, colStats, rowCount), estimateFilter(right, colStats, rowCount))
    case Or(left, right) => orCombine(estimateFilter(left, colStats, rowCount), estimateFilter(right, colStats, rowCount))
    case Not(child) => estimateFilter(child, colStats, rowCount) match {
      case Some(s) => Some(1.0 - s)
      case None    => None  // Not(unknown) = unknown → 1.0 (not 0.0)
    }
    case StringStartsWith(_, _) => Some(0.1)
    case _ => None  // unrecognised → unknown
  }

  /**
   * Linear interpolation selectivity for range predicates on numeric columns.
   *
   * @param upper if true, predicate is {@code > value} / {@code >= value}
   *              (kept fraction = (max - value) / range);
   *              if false, predicate is {@code < value} / {@code <= value}
   *              (kept fraction = (value - min) / range).
   *              The inclusive/exclusive distinction is ignored (Doris stats are
   *              approximate; the difference of one boundary point is negligible).
   * @return Some(selectivity) in [0, 1] when stats are available and numeric;
   *         None when stats are missing or non-numeric, so "unknown" propagates
   *         correctly through And/Or/Not.
   */
  private def rangeSelectivity(attr: String,
                               v: Any,
                               colStats: java.util.Map[String, DorisColumnStats],
                               upper: Boolean): Option[Double] = {
    val cs = colStats.get(attr)
    if (cs == null || cs.getMinLiteral == null || cs.getMaxLiteral == null) return None

    val minOpt = parseNumber(cs.getMinLiteral)
    val maxOpt = parseNumber(cs.getMaxLiteral)
    val valOpt = v match {
      case n: Number => Some(n.doubleValue())
      case _ => parseNumber(String.valueOf(v))
    }

    if (minOpt.isEmpty || maxOpt.isEmpty || valOpt.isEmpty) return None

    val min = minOpt.get
    val max = maxOpt.get
    val value = valOpt.get

    if (max <= min) return None

    val range = max - min
    val ratio = if (upper) (max - value) / range else (value - min) / range
    Some(clamp01(ratio))
  }

  // ------------------------------------------------------------------
  // Combinators that propagate "unknown" (None) correctly.
  // ------------------------------------------------------------------

  /** And: if one side is unknown, use the other; if both known, multiply. */
  private def andCombine(a: Option[Double], b: Option[Double]): Option[Double] = (a, b) match {
    case (Some(x), Some(y)) => Some(clamp01(x * y))
    case (Some(x), None)    => Some(x)
    case (None, Some(y))    => Some(y)
    case (None, None)       => None
  }

  /** Or: if one side is unknown, result is unknown (could be anywhere up to 1.0). */
  private def orCombine(a: Option[Double], b: Option[Double]): Option[Double] = (a, b) match {
    case (Some(x), Some(y)) => Some(clamp01(x + y - x * y))
    case _                  => None
  }

  private def withStats(attr: String, colStats: java.util.Map[String, DorisColumnStats], rowCount: Long)
                       (fn: DorisColumnStats => Option[Double]): Option[Double] = {
    val cs = colStats.get(attr)
    if (cs == null) return None
    fn(cs).filter(d => !d.isNaN && !d.isInfinite)
  }

  private def nullRatioOf(cs: DorisColumnStats, rowCount: Long): Double = {
    if (rowCount <= 0 || cs.getNumNulls < 0) 0.0
    else cs.getNumNulls.toDouble / rowCount
  }

  private def parseNumber(s: String): Option[Double] = {
    Try(s.trim.toDouble).toOption.filterNot(_.isNaN).filterNot(_.isInfinite)
  }

  private def clamp01(d: Double): Double = math.max(0.0, math.min(1.0, d))
}
