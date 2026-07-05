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

import java.util.OptionalLong

import org.apache.spark.sql.connector.read.Statistics

/**
 * Base {@link Statistics} implementation carrying only table-level metrics
 * (numRows and sizeInBytes). Subclasses in Spark 3.4+ may add column-level stats.
 */
private[spark] class DorisStatistics(
    private val numRowsOpt: OptionalLong,
    private val sizeInBytesOpt: OptionalLong) extends Statistics {

  override def numRows(): OptionalLong = numRowsOpt

  override def sizeInBytes(): OptionalLong = sizeInBytesOpt

  override def toString: String =
    s"DorisStatistics(numRows=$numRowsOpt, sizeInBytes=$sizeInBytesOpt)"
}

/**
 * Sentinel indicating no statistics are available. Spark will fall back to
 * its own default sizeInBytes estimate, matching pre-feature behavior.
 */
private[spark] object UnknownStatistics extends Statistics {
  private val EMPTY = OptionalLong.empty()
  override def numRows(): OptionalLong = EMPTY
  override def sizeInBytes(): OptionalLong = EMPTY
  override def toString: String = "UnknownStatistics"
}
