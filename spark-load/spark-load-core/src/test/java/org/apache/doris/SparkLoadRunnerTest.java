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

package org.apache.doris;

import org.apache.doris.config.JobConfig;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SparkLoadRunnerTest {

    @Test
    void loadHadoopConfig() {

        JobConfig jobConfig = new JobConfig();
        Map<String, String> envMap = new HashMap<>();
        envMap.put("HADOOP_CONF_DIR", this.getClass().getResource("/").getPath());
        jobConfig.setEnv(envMap);
        SparkLoadRunner.loadHadoopConfig(jobConfig);
        Assertions.assertEquals("hdfs://my-hadoop/", jobConfig.getHadoopProperties().get("fs.defaultFS"));
        Assertions.assertEquals("1", jobConfig.getHadoopProperties().get("dfs.replication"));
        Assertions.assertEquals("my.hadoop.com", jobConfig.getHadoopProperties().get("yarn.resourcemanager.address"));

    }
}