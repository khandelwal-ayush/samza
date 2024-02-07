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

package org.apache.samza.serializers.model.serializers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.diagnostics.BoundedList;
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestMetricsSnapshotSerdeV2 {
  // Process ID used for testing portable jobs schema
  private static final String DummyPortableProcessId = "Dummy";

  @Test
  public void testSerializeThenDeserialize() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(samzaException, true);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    byte[] serializedBytes = metricsSnapshotSerde.toBytes(metricsSnapshot);
    MetricsSnapshot deserializedMetricsSnapshot = metricsSnapshotSerde.fromBytes(serializedBytes);
    assertEquals(metricsSnapshot, deserializedMetricsSnapshot);
  }

  @Test
  public void testSerializeThenDeserializeEmptySamzaEpochIdInHeader() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(samzaException, false);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    byte[] serializedBytes = metricsSnapshotSerde.toBytes(metricsSnapshot);
    MetricsSnapshot deserializedMetricsSnapshot = metricsSnapshotSerde.fromBytes(serializedBytes);
    assertEquals(metricsSnapshot, deserializedMetricsSnapshot);
  }

  /**
   * Helps for verifying compatibility when schemas evolve.
   *
   * Maps have non-deterministic ordering when serialized, so it is difficult to check exact serialized results. It
   * isn't really necessary to check the serialized results anyways. We just need to make sure serialized data can be
   * read by old and new systems.
   */
  @Test
  public void testDeserializeRaw() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(samzaException, true);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    assertEquals(metricsSnapshot, metricsSnapshotSerde.fromBytes(
        expectedSeralizedSnapshot(samzaException, true, false).getBytes(StandardCharsets.UTF_8)));
    assertEquals(metricsSnapshot, metricsSnapshotSerde.fromBytes(
        expectedSeralizedSnapshot(samzaException, true, true).getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Helps for verifying compatibility when schemas evolve.
   */
  @Test
  public void testDeserializeRawEmptySamzaEpochIdInHeader() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(samzaException, false);
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    assertEquals(metricsSnapshot, metricsSnapshotSerde.fromBytes(
        expectedSeralizedSnapshot(samzaException, false, false).getBytes(StandardCharsets.UTF_8)));
    assertEquals(metricsSnapshot, metricsSnapshotSerde.fromBytes(
        expectedSeralizedSnapshot(samzaException, false, true).getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testDeserializeRawPortableFieldsInHeader() {
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));

    // 1 - Test snapshot with leaving the portable fields empty.
    MetricsSnapshot snapshotWithoutBoth =
        metricsSnapshot(samzaException, true, true, null, null);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotWithoutBoth, true, null, null);

    // 2 - Test snapshot with only leaving isPortableJob field empty.
    MetricsSnapshot snapshotWithNullIsPortable =
        metricsSnapshot(samzaException, true, true, null, DummyPortableProcessId);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotWithNullIsPortable, true, null, DummyPortableProcessId);

    // 3 - Test snapshot with only leaving portableProcessId field empty.
    MetricsSnapshot snapshotWithNullProcessId =
        metricsSnapshot(samzaException, true, true, true, null);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotWithNullProcessId, true, true, null);

    // 4 - Test snapshot with both portable fields filled.
    MetricsSnapshot snapshotForPortableJob =
        metricsSnapshot(samzaException, true, true, true, DummyPortableProcessId);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotForPortableJob, true, true, DummyPortableProcessId);

    // 5 - Test snapshot with both portable fields filled and without Samza Epoch Id.
    MetricsSnapshot snapshotForPortableJobWithoutEpoch =
        metricsSnapshot(samzaException, false, true, true, DummyPortableProcessId);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotForPortableJobWithoutEpoch, false, true, DummyPortableProcessId);

    // 6 - Test snapshot without any portable fields but including epoch ID
    MetricsSnapshot snapshotForNonPortableJobWithEpoch =
        metricsSnapshot(samzaException, true, false, null, null);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotForNonPortableJobWithEpoch, true, null, null);

    // 7 - Test snapshot without any portable fields but including epoch ID
    MetricsSnapshot snapshotForNonPortableJobWithoutEpoch =
        metricsSnapshot(samzaException, false, false, null, null);
    verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(samzaException, snapshotForNonPortableJobWithoutEpoch, false, null, null);
  }

  private void verifyMetricSnapshotForPortableFieldsWithAndWithoutExtraHeader(
      Exception samzaException,
      MetricsSnapshot snapshotToVerify,
      boolean includeSamzaEpochId,
      Boolean isPortableJob,
      String portableJobProcessId) {

    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();

    // 1 - Verify without extra header
    String expectedWithoutBothFields = expectedSeralizedSnapshot(
        samzaException, includeSamzaEpochId, isPortableJob, portableJobProcessId, false);
    assertEquals(
        snapshotToVerify,
        metricsSnapshotSerde.fromBytes(expectedWithoutBothFields.getBytes(StandardCharsets.UTF_8)));
    // 2 - Verify with extra header
    String expectedWithoutBothFieldsExtra = expectedSeralizedSnapshot(
        samzaException, includeSamzaEpochId, isPortableJob, portableJobProcessId, true);
    assertEquals(
        snapshotToVerify,
        metricsSnapshotSerde.fromBytes(expectedWithoutBothFieldsExtra.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testPortableJobsSchemaUsingLatestSchema() {
    // ARRANGE
    MetricsSnapshotSerdeV2 metricsSnapshotSerde = new MetricsSnapshotSerdeV2();
    SamzaException samzaException = new SamzaException("this is a samza exception", new RuntimeException("cause"));
    MetricsSnapshot metricsSnapshot = metricsSnapshot(
        samzaException, true, true, true, DummyPortableProcessId);

    // 1) Verify without extra header
    String expectedString = expectedSeralizedSnapshot(samzaException, true, true, DummyPortableProcessId, false);
    byte[] expectedBytes = expectedString.getBytes(StandardCharsets.UTF_8);
    MetricsSnapshot expectedSnapshot = metricsSnapshotSerde.fromBytes(expectedBytes);
    assertEquals(expectedSnapshot, metricsSnapshot);

    // 2) Verify with extra header
    expectedString = expectedSeralizedSnapshot(samzaException, true, true, DummyPortableProcessId, true);
    expectedBytes = expectedString.getBytes(StandardCharsets.UTF_8);
    expectedSnapshot = metricsSnapshotSerde.fromBytes(expectedBytes);
    assertEquals(expectedSnapshot, metricsSnapshot);
  }

  /**
   * Create dummy Metric Header for testing
   * @param includePortableJobFields Whether to include latest schema updates to support portable jobs
   * @param includeSamzaEpochId Include Samza Epoch Id
   * @param isPortableJob Whether it is a portable job. Set only if `includePortableJobFields` is set to true.
   * @param portableJobProcessId Process ID for portable job. Set only if `includePortableJobFields` is set to true.
   * @return Dummy Metric Header
   */
  private static MetricsHeader createMetricsHeader(
      boolean includeSamzaEpochId,
      boolean includePortableJobFields,
      Boolean isPortableJob,
      String portableJobProcessId) {
    if (!includeSamzaEpochId && !includePortableJobFields) {
      return new MetricsHeader(
          "jobName", "i001", "container 0", "test container ID",
          "source", "300.14.25.1", "1", "1", 1, 1);
    } else if (!includePortableJobFields) {
      return new MetricsHeader("jobName", "i001", "container 0", "test container ID",
          Optional.of("epoch-123"), "source", "300.14.25.1", "1", "1", 1, 1);
    } else {
      return new MetricsHeader("jobName", "i001", "container 0", "test container ID",
          includeSamzaEpochId
              ? Optional.of("epoch-123")
              : Optional.empty(),
          "source", "300.14.25.1", "1", "1", 1, 1,
          isPortableJob != null
              ? Optional.of(isPortableJob)
              : Optional.empty(),
          portableJobProcessId != null
              ? Optional.of(portableJobProcessId)
              : Optional.empty());
    }
  }

  private static MetricsSnapshot metricsSnapshot(
      Exception exception, boolean includeSamzaEpochId) {
    return metricsSnapshot(exception, includeSamzaEpochId, false, false, null);
  }

    private static MetricsSnapshot metricsSnapshot(
        Exception exception,
        boolean includeSamzaEpochId,
        boolean includePortableJobFields,
        Boolean isPortableJob,
        String portableJobProcessId) {
    MetricsHeader metricsHeader = createMetricsHeader(
        includeSamzaEpochId, includePortableJobFields, isPortableJob, portableJobProcessId);
    BoundedList<DiagnosticsExceptionEvent> boundedList = new BoundedList<>("exceptions");
    DiagnosticsExceptionEvent diagnosticsExceptionEvent = new DiagnosticsExceptionEvent(1, exception, new HashMap<>());
    boundedList.add(diagnosticsExceptionEvent);
    Map<String, Map<String, Object>> metricMessage = new HashMap<>();
    Map<String, Object> samzaContainerMetrics = new HashMap<>();
    samzaContainerMetrics.put("commit-calls", 1);
    metricMessage.put("org.apache.samza.container.SamzaContainerMetrics", samzaContainerMetrics);
    Map<String, Object> exceptions = new HashMap<>();
    exceptions.put("exceptions", boundedList.getValues());
    metricMessage.put("org.apache.samza.exceptions", exceptions);
    return new MetricsSnapshot(metricsHeader, new Metrics(metricMessage));
  }

  /**
   * @param includeSamzaEpochId include the new samza-epoch-id field (for testing that new code can read old data
   *                           without this field)
   * @param includeExtraHeaderField include an extra new field (for testing that old code can read new data with extra
   *                                fields)
   */
  private static String expectedSeralizedSnapshot(Exception exception, boolean includeSamzaEpochId,
      boolean includeExtraHeaderField) {
    return expectedSeralizedSnapshot(
        exception, includeSamzaEpochId, null, null, includeExtraHeaderField);
  }

  private static String expectedSeralizedSnapshot(Exception exception, boolean includeSamzaEpochId,
      Boolean isPortableJob, String portableJobProcessId,
      boolean includeExtraHeaderField) {
    String stackTrace = ExceptionUtils.getStackTrace(exception);
    String serializedSnapshot =
        "{\"header\":[\"java.util.HashMap\",{\"job-id\":\"i001\",\"exec-env-container-id\":\"test container ID\",";

    if (includeSamzaEpochId) {
      serializedSnapshot += "\"samza-epoch-id\":\"epoch-123\",";
    }

    if (isPortableJob != null) {
      serializedSnapshot += "\"is-portable-job\":[\"java.lang.Boolean\"," + isPortableJob + "],";
    }

    if (portableJobProcessId != null) {
      serializedSnapshot += "\"portable-job-process-id\":\"" + portableJobProcessId + "\",";
    }

    if (includeExtraHeaderField) {
      serializedSnapshot += "\"extra-header-field\":\"extra header value\",";
    }
    // in serialized string, backslash in whitespace characters (e.g. \n, \t) are escaped
    String escapedStackTrace = stackTrace.replace("\n", "\\n").replace("\t", "\\t");
    serializedSnapshot +=
        "\"samza-version\":\"1\",\"job-name\":\"jobName\",\"host\":\"1\",\"reset-time\":[\"java.lang.Long\",1],"
            + "\"container-name\":\"container 0\",\"source\":\"source\",\"time\":[\"java.lang.Long\",1],\"version\":\"300.14.25.1\"}],"
            + "\"metrics\":[\"java.util.HashMap\",{\"org.apache.samza.exceptions\":"
            + "[\"java.util.HashMap\",{\"exceptions\":[\"java.util.Collections$UnmodifiableRandomAccessList\","
            + "[[\"org.apache.samza.diagnostics.DiagnosticsExceptionEvent\",{\"timestamp\":1,\"exceptionType\":\"org.apache.samza.SamzaException\","
            + "\"exceptionMessage\":\"this is a samza exception\",\"compactExceptionStackTrace\":\"" + escapedStackTrace
            + "\",\"mdcMap\":[\"java.util.HashMap\",{}]}]]]}],\"org.apache.samza.container.SamzaContainerMetrics\":"
            + "[\"java.util.HashMap\",{\"commit-calls\":1}]}]}";
    return serializedSnapshot;
  }


}
