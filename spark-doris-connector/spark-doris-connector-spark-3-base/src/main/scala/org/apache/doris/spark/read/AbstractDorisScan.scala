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

package org.apache.doris.spark.read

import org.apache.doris.spark.client.DorisFrontendClient
import org.apache.doris.spark.client.entity.{Backend, DorisColumnStats, DorisReaderPartition}
import org.apache.doris.spark.client.read.ReaderPartitionGenerator
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.read.stats.{DorisStatistics, SelectivityEstimator, UnknownStatistics}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, SupportsReportStatistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal
import java.util.{Collections, OptionalLong}
import scala.collection.JavaConverters._
import scala.language.implicitConversions

abstract class AbstractDorisScan(config: DorisConfig, schema: StructType)
    extends Scan with Batch with SupportsReportStatistics with Logging {

  private val scanMode = ScanMode.valueOf(config.getValue(DorisOptions.READ_MODE).toUpperCase)

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    ReaderPartitionGenerator.generatePartitions(config, schema.names, compiledFilters(), getLimit,
      SQLConf.get.datetimeJava8ApiEnabled)
      .map(toInputPartition)
  }


  override def createReaderFactory(): PartitionReaderFactory = {
    new DorisPartitionReaderFactory(readSchema(), scanMode, config)
  }

  private def toInputPartition(rp: DorisReaderPartition): DorisInputPartition =
    DorisInputPartition(rp.getDatabase, rp.getTable, rp.getBackend, rp.getTablets.map(_.toLong), rp.getOpaquedQueryPlan,
      rp.getReadColumns, rp.getFilters, rp.getLimit, rp.getDateTimeJava8APIEnabled)

  protected def compiledFilters(): Array[String]

  protected def getLimit: Int = -1

  /**
   * Return V1 [[Filter]]s for selectivity estimation. Version-specific scan classes
   * that receive V2 predicates must convert them here; subclasses using V1 filters
   * can return them directly.
   */
  protected def selectivityFilters(): Array[Filter] = Array.empty

  // ------------------------------------------------------------------
  // SupportsReportStatistics
  // ------------------------------------------------------------------

  override def estimateStatistics(): org.apache.spark.sql.connector.read.Statistics = {
    if (!config.getValue(DorisOptions.DORIS_STATS_ENABLED)) {
      return UnknownStatistics
    }

    // Top-level guard: stats must never break the read path.
    // Any unforeseen exception (FE init, HTTP, parsing, ...) is swallowed here.
    try {
      estimateStatisticsInternal()
    } catch {
      case NonFatal(e) =>
        logWarning(s"estimateStatistics failed, falling back to unknown stats: ${e.getMessage}", e)
        UnknownStatistics
    }
  }

  private def estimateStatisticsInternal(): org.apache.spark.sql.connector.read.Statistics = {
    val (db, table) = parseTableIdentifier() match {
      case Some(pair) => pair
      case None => return UnknownStatistics
    }

    // DorisFrontendClient owns a CloseableHttpClient in auto-fetch mode; ensure it
    // is released to avoid leaking one HTTP client per query planning.
    val client = new DorisFrontendClient(config)
    try {
      val tblStats = client.fetchTableStats(db, table).orElse(null)
      if (tblStats == null || tblStats.getRowCount < 0) {
        return UnknownStatistics
      }

      val rowCount = tblStats.getRowCount
      // rowCount == 0 (e.g. ANALYZE never run, or truly empty table): report unknown
      // so Catalyst uses its default sizeInBytes. A stale 0 on a big table would
      // otherwise trigger broadcast join OOM.
      if (rowCount == 0) {
        return UnknownStatistics
      }

      val readCols = schema.names.toList
      val colStatsEnabled = config.getValue(DorisOptions.DORIS_STATS_COLUMN_ENABLED)
      val selectivityEnabled = config.getValue(DorisOptions.DORIS_STATS_SELECTIVITY_ENABLED)

      val colStatsMap: java.util.Map[String, DorisColumnStats] =
        if (colStatsEnabled || selectivityEnabled) {
          client.fetchColumnStats(db, table, readCols.asJava)
        } else {
          Collections.emptyMap()
        }

      // sizeInBytes: prefer sum of read-column data_size, fall back to table-level.
      var sizeInBytes = 0L
      val colIter = colStatsMap.values().iterator()
      while (colIter.hasNext) {
        val cs = colIter.next()
        if (readCols.contains(cs.getColumn) && cs.getDataSize > 0) {
          sizeInBytes += cs.getDataSize
        }
      }
      if (sizeInBytes <= 0) {
        sizeInBytes = tblStats.getDataSizeBytes
      }
      if (sizeInBytes <= 0 && rowCount > 0) {
        // rough fallback: assume ~100 bytes/row
        sizeInBytes = rowCount * 100L
      }

      // selectivity
      val selectivity =
        if (selectivityEnabled && rowCount > 0) {
          val raw = SelectivityEstimator.estimate(selectivityFilters(), colStatsMap, rowCount)
          val minSel = config.getValue(DorisOptions.DORIS_STATS_SELECTIVITY_MIN)
          math.max(minSel, raw)
        } else {
          1.0
        }

      // filter first, then limit (Doris semantics: WHERE ... LIMIT)
      val filteredRows = math.round(rowCount * selectivity)
      val effectiveRows =
        if (getLimit > 0) math.min(filteredRows, getLimit.toLong)
        else filteredRows

      val effectiveBytes =
        if (rowCount > 0) math.round(sizeInBytes * (effectiveRows.toDouble / rowCount))
        else sizeInBytes

      // Clamp to non-negative; a negative value would confuse Catalyst.
      val safeRows = math.max(0L, effectiveRows)
      val safeBytes = math.max(0L, effectiveBytes)

      logDebug(s"estimateStatistics: rowCount=$rowCount, selectivity=$selectivity, " +
          s"effectiveRows=$safeRows, effectiveBytes=$safeBytes, colStats=${colStatsMap.size()}")

      buildStatistics(
        OptionalLong.of(safeRows),
        OptionalLong.of(safeBytes),
        if (colStatsEnabled) colStatsMap else Collections.emptyMap(),
        safeRows)
    } finally {
      try {
        client.close()
      } catch {
        case NonFatal(e) => logDebug(s"close stats client failed: ${e.getMessage}")
      }
    }
  }

  /**
   * Build the version-appropriate {@link Statistics} object. The base implementation
   * returns table-level only (numRows + sizeInBytes). Spark 3.4+ subclasses override
   * to also populate column-level statistics.
   *
   * @param effectiveRows the row count after selectivity/limit; column stats should
   *                      cap nullCount/distinctCount to this to stay consistent
   */
  protected def buildStatistics(
      numRows: OptionalLong,
      sizeInBytes: OptionalLong,
      colStats: java.util.Map[String, DorisColumnStats],
      effectiveRows: Long): org.apache.spark.sql.connector.read.Statistics = {
    new DorisStatistics(numRows, sizeInBytes)
  }

  private def parseTableIdentifier(): Option[(String, String)] = {
    try {
      val ident = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
      val parts = ident.split("\\.").map(_.replaceAll("`", ""))
      if (parts.length >= 2) Some((parts(0), parts(1))) else None
    } catch {
      case _: Exception => None
    }
  }
}

case class DorisInputPartition(database: String, table: String, backend: Backend, tablets: Array[Long],
                               opaquedQueryPlan: String, readCols: Array[String], predicates: Array[String],
                               limit: Int = -1, datetimeJava8ApiEnabled: Boolean) extends InputPartition
