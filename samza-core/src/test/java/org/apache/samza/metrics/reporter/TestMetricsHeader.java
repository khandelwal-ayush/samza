/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.metrics.reporter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestMetricsHeader {
  private static final String JOB_NAME = "job-a";
  private static final String JOB_ID = "id-a";
  private static final String CONTAINER_NAME = "samza-container-0";
  private static final String EXEC_ENV_CONTAINER_ID = "container-12345";
  private static final String SAMZA_EPOCH_ID = "epoch-12345";
  private static final String SOURCE = "metrics-source";
  private static final String VERSION = "1.2.3";
  private static final String SAMZA_VERSION = "4.5.6";
  private static final String HOST = "host0.a.b.c";
  private static final long TIME = 100;
  private static final long RESET_TIME = 10;
  private static final Boolean IS_PORTABLE_JOB = true;
  private static final MetricsHeader.PortableJobFields.ProcessType PORTABLE_JOB_PROCESS_TYPE
      = MetricsHeader.PortableJobFields.ProcessType.Runner;
  private static final String PORTABLE_JOB_PROCESS_ID = "1033";

  @Test
  public void testGetAsMap() {

    short schemaVersion = 1;
    MetricsHeader.PortableJobFields portableJobFields = new MetricsHeader.PortableJobFields(
        IS_PORTABLE_JOB, PORTABLE_JOB_PROCESS_TYPE, PORTABLE_JOB_PROCESS_ID);

    MetricsHeader metricsHeader =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, Optional.of(SAMZA_EPOCH_ID),
            SOURCE, VERSION, SAMZA_VERSION, HOST, TIME, RESET_TIME, Optional.of(schemaVersion), Optional.of(portableJobFields));
    Map<String, Object> expected = new HashMap<>();
    expected.put("job-name", JOB_NAME);
    expected.put("job-id", JOB_ID);
    expected.put("container-name", CONTAINER_NAME);
    expected.put("exec-env-container-id", EXEC_ENV_CONTAINER_ID);
    expected.put("samza-epoch-id", SAMZA_EPOCH_ID);
    expected.put("source", SOURCE);
    expected.put("version", VERSION);
    expected.put("samza-version", SAMZA_VERSION);
    expected.put("host", HOST);
    expected.put("time", TIME);
    expected.put("reset-time", RESET_TIME);
    expected.put("metrics-schema-version", schemaVersion);
    expected.put("portable-job-fields", portableJobFields);
    assertEquals(expected, metricsHeader.getAsMap());

    // test with empty samza epoch id
    metricsHeader =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, Optional.empty(), SOURCE, VERSION,
            SAMZA_VERSION, HOST, TIME, RESET_TIME, Optional.of((short)1), Optional.of(portableJobFields));
    expected.remove("samza-epoch-id");
    assertEquals(expected, metricsHeader.getAsMap());

    // test with empty metrics schema version and portable fields
    metricsHeader =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, Optional.empty(), SOURCE, VERSION,
            SAMZA_VERSION, HOST, TIME, RESET_TIME, Optional.empty(), Optional.empty());

    expected.remove("metrics-schema-version");
    expected.remove("portable-job-fields");
    assertEquals(expected, metricsHeader.getAsMap());
  }

  @Test
  public void testFromMap() {

    Optional<Short> schemaVersion = Optional.of((short)1);
    Optional<MetricsHeader.PortableJobFields> portableJobFields = Optional.of(new MetricsHeader.PortableJobFields(
        IS_PORTABLE_JOB, PORTABLE_JOB_PROCESS_TYPE, PORTABLE_JOB_PROCESS_ID));

    Map<String, Object> map = new HashMap<>();
    map.put("job-name", JOB_NAME);
    map.put("job-id", JOB_ID);
    map.put("container-name", CONTAINER_NAME);
    map.put("exec-env-container-id", EXEC_ENV_CONTAINER_ID);
    map.put("samza-epoch-id", SAMZA_EPOCH_ID);
    map.put("source", SOURCE);
    map.put("version", VERSION);
    map.put("samza-version", SAMZA_VERSION);
    map.put("host", HOST);
    map.put("time", TIME);
    map.put("reset-time", RESET_TIME);
    map.put("metrics-schema-version", schemaVersion);
    map.put("portable-job-fields", portableJobFields);

    MetricsHeader expected =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, Optional.of(SAMZA_EPOCH_ID),
            SOURCE, VERSION, SAMZA_VERSION, HOST, TIME, RESET_TIME,
            schemaVersion,
            portableJobFields);
    MetricsHeader actual = MetricsHeader.fromMap(map);
    assertEquals(expected, actual);

    // test with missing samza epoch id
    map.remove("samza-epoch-id");
    expected =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, Optional.empty(), SOURCE, VERSION,
            SAMZA_VERSION, HOST, TIME, RESET_TIME, schemaVersion, portableJobFields);
    assertEquals(expected, MetricsHeader.fromMap(map));

    // test with missing portable job data
    map.remove("metrics-schema-version");
    map.remove("portable-job-fields");
    expected =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, Optional.empty(), SOURCE, VERSION,
            SAMZA_VERSION, HOST, TIME, RESET_TIME, Optional.empty(), Optional.empty());
    assertEquals(expected, MetricsHeader.fromMap(map));
  }
}
