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
 * Column-level statistics fetched from Doris via {@code SHOW COLUMN STATS}.
 *
 * <p>Fields default to {@code -1} / {@code null} when the underlying Doris version
 * does not surface them.
 *
 * <p>{@code minLiteral} / {@code maxLiteral} are kept as raw strings; consumers on the
 * Spark side must parse them into Catalyst-internal representations according to the
 * Spark schema (e.g. {@code DateType} -> epoch-day {@code Int}).
 */
public final class DorisColumnStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String column;
    private final long ndv;
    private final long numNulls;
    private final long dataSize;
    private final double avgSizeByte;
    private final String minLiteral;
    private final String maxLiteral;

    public DorisColumnStats(String column,
                            long ndv,
                            long numNulls,
                            long dataSize,
                            double avgSizeByte,
                            String minLiteral,
                            String maxLiteral) {
        this.column = column;
        this.ndv = ndv;
        this.numNulls = numNulls;
        this.dataSize = dataSize;
        this.avgSizeByte = avgSizeByte;
        this.minLiteral = minLiteral;
        this.maxLiteral = maxLiteral;
    }

    public String getColumn() {
        return column;
    }

    public long getNdv() {
        return ndv;
    }

    public long getNumNulls() {
        return numNulls;
    }

    public long getDataSize() {
        return dataSize;
    }

    public double getAvgSizeByte() {
        return avgSizeByte;
    }

    public String getMinLiteral() {
        return minLiteral;
    }

    public String getMaxLiteral() {
        return maxLiteral;
    }

    @Override
    public String toString() {
        return "DorisColumnStats{column='" + column + '\''
                + ", ndv=" + ndv
                + ", numNulls=" + numNulls
                + ", dataSize=" + dataSize
                + ", avgSizeByte=" + avgSizeByte
                + ", minLiteral='" + minLiteral + '\''
                + ", maxLiteral='" + maxLiteral + '\''
                + '}';
    }
}
