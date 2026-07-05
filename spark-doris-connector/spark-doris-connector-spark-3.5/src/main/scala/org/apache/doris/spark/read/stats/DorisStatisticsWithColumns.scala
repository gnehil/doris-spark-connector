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

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

import java.{util => ju}
import scala.util.Try

/**
 * {@link Statistics} implementation that also reports column-level statistics.
 * Used by Spark 3.4+ where [[Statistics.columnStats()]] is available.
 */
private[spark] class DorisStatisticsWithColumns(
    numRowsOpt: ju.OptionalLong,
    sizeInBytesOpt: ju.OptionalLong,
    colStats: ju.Map[String, DorisColumnStats],
    schema: StructType,
    effectiveRows: Long)
    extends DorisStatistics(numRowsOpt, sizeInBytesOpt) {

  /**
   * Build the columnStats map expected by Spark 3.4+.
   *
   * <p>min/max are only reported for numeric, date, timestamp, and decimal types,
   * and are converted to Catalyst-internal representations (e.g. DateType -> epoch-day Int).
   * String columns do not report min/max (matching Spark's own ANALYZE behavior).
   *
   * <p>nullCount and distinctCount are capped at effectiveRows so they never exceed
   * the (post-selectivity / post-limit) reported numRows.
   */
  override def columnStats(): ju.Map[NamedReference, ColumnStatistics] = {
    if (colStats == null || colStats.isEmpty) {
      return ju.Collections.emptyMap()
    }
    val result = new ju.HashMap[NamedReference, ColumnStatistics]()
    val fieldMap = schema.fields.map(f => f.name -> f.dataType).toMap

    val it = colStats.values().iterator()
    while (it.hasNext) {
      val cs = it.next()
      val colName = cs.getColumn
      if (colName != null && fieldMap.contains(colName)) {
        val sparkType = fieldMap(colName)
        val ref = new SimpleNamedReference(colName)
        val colStat = buildColumnStat(cs, sparkType)
        result.put(ref, colStat)
      }
    }
    result
  }

  private def buildColumnStat(cs: DorisColumnStats, sparkType: DataType): ColumnStatistics = {
    // Cap to effectiveRows so column stats stay consistent with the reported numRows.
    val distinctCount =
      if (cs.getNdv >= 0) ju.OptionalLong.of(math.min(cs.getNdv, effectiveRows))
      else ju.OptionalLong.empty()
    val nullCount =
      if (cs.getNumNulls >= 0) ju.OptionalLong.of(math.min(cs.getNumNulls, effectiveRows))
      else ju.OptionalLong.empty()
    val avgLen =
      if (cs.getAvgSizeByte >= 0) ju.OptionalLong.of(math.round(cs.getAvgSizeByte).toLong)
      else ju.OptionalLong.empty()
    val maxLen = ju.OptionalLong.empty()

    val (minOpt, maxOpt) = parseMinMax(cs, sparkType)

    new ColumnStatistics {
      override def min(): ju.Optional[Object] = minOpt
      override def max(): ju.Optional[Object] = maxOpt
      override def distinctCount(): ju.OptionalLong = distinctCount
      override def nullCount(): ju.OptionalLong = nullCount
      override def avgLen(): ju.OptionalLong = avgLen
      override def maxLen(): ju.OptionalLong = maxLen
    }
  }

  private def parseMinMax(cs: DorisColumnStats,
                          sparkType: DataType): (ju.Optional[Object], ju.Optional[Object]) = {
    if (cs.getMinLiteral == null || cs.getMaxLiteral == null) {
      return (ju.Optional.empty[Object](), ju.Optional.empty[Object]())
    }
    sparkType match {
      case IntegerType | ShortType =>
        val minV = Try(java.lang.Integer.valueOf(cs.getMinLiteral.trim)).toOption
        val maxV = Try(java.lang.Integer.valueOf(cs.getMaxLiteral.trim)).toOption
        (optBox(minV), optBox(maxV))
      case LongType =>
        val minV = Try(java.lang.Long.valueOf(cs.getMinLiteral.trim)).toOption
        val maxV = Try(java.lang.Long.valueOf(cs.getMaxLiteral.trim)).toOption
        (optBox(minV), optBox(maxV))
      case FloatType =>
        val minV = Try(java.lang.Float.valueOf(cs.getMinLiteral.trim)).toOption
        val maxV = Try(java.lang.Float.valueOf(cs.getMaxLiteral.trim)).toOption
        (optBox(minV), optBox(maxV))
      case DoubleType =>
        val minV = Try(java.lang.Double.valueOf(cs.getMinLiteral.trim)).toOption
        val maxV = Try(java.lang.Double.valueOf(cs.getMaxLiteral.trim)).toOption
        (optBox(minV), optBox(maxV))
      case _: DecimalType =>
        val minV = Try(new java.math.BigDecimal(cs.getMinLiteral.trim)).toOption
        val maxV = Try(new java.math.BigDecimal(cs.getMaxLiteral.trim)).toOption
        (optBox(minV), optBox(maxV))
      case DateType =>
        val minV = Try(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(cs.getMinLiteral.trim)): java.lang.Integer).toOption
        val maxV = Try(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(cs.getMaxLiteral.trim)): java.lang.Integer).toOption
        (optBox(minV), optBox(maxV))
      case TimestampType =>
        val minV = Try(DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(cs.getMinLiteral.trim)): java.lang.Long).toOption
        val maxV = Try(DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(cs.getMaxLiteral.trim)): java.lang.Long).toOption
        (optBox(minV), optBox(maxV))
      case StringType | _ =>
        (ju.Optional.empty[Object](), ju.Optional.empty[Object]())
    }
  }

  private def optBox[T <: Object](opt: Option[T]): ju.Optional[T] = opt match {
    case Some(v) => ju.Optional.of(v)
    case None => ju.Optional.empty[T]()
  }
}

/** Minimal NamedReference implementation for column stats keys. */
private[spark] class SimpleNamedReference(colName: String) extends NamedReference {
  override def fieldNames(): Array[String] = Array(colName)
  override def describe(): String = colName
  override def toString: String = colName
}
