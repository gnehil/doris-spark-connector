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

package org.apache.doris.spark.client.entity;

import java.io.Serializable;

/**
 * Table-level statistics fetched from Doris via {@code SHOW TABLE STATS}.
 *
 * <p>{@code rowCount} is the authoritative field. {@code dataSizeBytes} is optional
 * ({@literal <} 0 when Doris does not surface it); callers should fall back to
 * {@code SUM(column data_size)} in that case.
 */
public final class DorisTableStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long rowCount;
    private final long dataSizeBytes;

    public DorisTableStats(long rowCount, long dataSizeBytes) {
        this.rowCount = rowCount;
        this.dataSizeBytes = dataSizeBytes;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getDataSizeBytes() {
        return dataSizeBytes;
    }

    @Override
    public String toString() {
        return "DorisTableStats{rowCount=" + rowCount + ", dataSizeBytes=" + dataSizeBytes + '}';
    }
}
