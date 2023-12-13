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
import java.util.Objects;
import java.util.Optional;


public class MetricsHeader {
  // Schema version for the metrics header.
  public static final short METRICS_SCHEMA_VERSION = 1;
  // Process ID for Runner process. This is used for portable jobs.
  public static final String PORTABLE_JOB_RUNNER_PROCESS_ID = "Runner";

  private static final String JOB_NAME = "job-name";
  private static final String JOB_ID = "job-id";
  private static final String CONTAINER_NAME = "container-name";
  private static final String EXEC_ENV_CONTAINER_ID = "exec-env-container-id";
  private static final String SAMZA_EPOCH_ID = "samza-epoch-id";
  private static final String SOURCE = "source";
  private static final String VERSION = "version";
  private static final String SAMZA_VERSION = "samza-version";
  private static final String HOST = "host";
  private static final String TIME = "time";
  private static final String RESET_TIME = "reset-time";
  private static final String METRICS_SCHEMA_VERSION_LABEL = "metrics-schema-version";
  private static final String PORTABLE_JOB_FIELDS = "portable-job-fields";
  private final String jobName;
  private final String jobId;
  private final String containerName;
  private final String execEnvironmentContainerId;
  /**
   * This is optional for backwards compatibility. It was added after the initial version of this class.
   */
  private final Optional<String> samzaEpochId;
  private final String source;
  private final String version;
  private final String samzaVersion;
  private final String host;
  private final long time;
  private final long resetTime;
  private final Optional<Short> schemaVersion;
  private final Optional<PortableJobFields> portableJobFields;

  public MetricsHeader(String jobName, String jobId, String containerName, String execEnvironmentContainerId,
      String source, String version, String samzaVersion, String host, long time, long resetTime) {
    this(jobName, jobId, containerName, execEnvironmentContainerId, Optional.empty(), source, version, samzaVersion,
        host, time, resetTime);
  }

  public MetricsHeader(String jobName, String jobId, String containerName, String execEnvironmentContainerId,
      Optional<String> samzaEpochId, String source, String version, String samzaVersion, String host, long time,
      long resetTime) {
    this(jobName, jobId, containerName, execEnvironmentContainerId, samzaEpochId, source, version, samzaVersion,
        host, time, resetTime, Optional.empty(), Optional.empty());
  }

  public MetricsHeader(String jobName, String jobId, String containerName, String execEnvironmentContainerId,
      Optional<String> samzaEpochId, String source, String version, String samzaVersion, String host, long time,
      long resetTime, Optional<Short> schemaVersion, Optional<PortableJobFields> portableJobFields) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.containerName = containerName;
    this.execEnvironmentContainerId = execEnvironmentContainerId;
    this.samzaEpochId = samzaEpochId;
    this.source = source;
    this.version = version;
    this.samzaVersion = samzaVersion;
    this.host = host;
    this.time = time;
    this.resetTime = resetTime;
    this.schemaVersion = schemaVersion;
    this.portableJobFields = portableJobFields;
  }

  public Map<String, Object> getAsMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(JOB_NAME, jobName);
    map.put(JOB_ID, jobId);
    map.put(CONTAINER_NAME, containerName);
    map.put(EXEC_ENV_CONTAINER_ID, execEnvironmentContainerId);
    this.samzaEpochId.ifPresent(epochId -> map.put(SAMZA_EPOCH_ID, epochId));
    map.put(SOURCE, source);
    map.put(VERSION, version);
    map.put(SAMZA_VERSION, samzaVersion);
    map.put(HOST, host);
    map.put(TIME, time);
    map.put(RESET_TIME, resetTime);
    this.schemaVersion.ifPresent(schemaVersion -> map.put(METRICS_SCHEMA_VERSION_LABEL, schemaVersion));
    this.portableJobFields.ifPresent(portableJobFields -> map.put(PORTABLE_JOB_FIELDS, portableJobFields));
    return map;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobId() {
    return jobId;
  }

  public String getContainerName() {
    return containerName;
  }

  public String getExecEnvironmentContainerId() {
    return execEnvironmentContainerId;
  }

  /**
   * Epoch id for the application, which is consistent across all components of a single deployment attempt.
   * This may be empty, since this field was added to this class after the initial version.
   */
  public Optional<String> getSamzaEpochId() {
    return samzaEpochId;
  }

  public String getSource() {
    return source;
  }

  public String getVersion() {
    return version;
  }

  public String getSamzaVersion() {
    return samzaVersion;
  }

  public String getHost() {
    return host;
  }

  public long getTime() {
    return time;
  }

  public long getResetTime() {
    return resetTime;
  }

  public Optional<Short> getSchemaVersion() { return schemaVersion; }

  public Optional<PortableJobFields> getPortableJobFields() { return portableJobFields; }

  public static MetricsHeader fromMap(Map<String, Object> map) {

    Optional<PortableJobFields> portableJobFields = Optional.empty();
    if (map.containsKey(PORTABLE_JOB_FIELDS)) {
      portableJobFields = Optional.of((PortableJobFields) map.get(PORTABLE_JOB_FIELDS));
    }

    Optional<Short> schemaVersion = Optional.empty();
    if (map.containsKey(METRICS_SCHEMA_VERSION_LABEL)) {
      schemaVersion = Optional.of((Short) map.get(METRICS_SCHEMA_VERSION_LABEL));
    }

    return new MetricsHeader(map.get(JOB_NAME).toString(), map.get(JOB_ID).toString(),
        map.get(CONTAINER_NAME).toString(), map.get(EXEC_ENV_CONTAINER_ID).toString(),
        // need to check existence for backwards compatibility with initial version of this class
        Optional.ofNullable(map.get(SAMZA_EPOCH_ID)).map(Object::toString), map.get(SOURCE).toString(),
        map.get(VERSION).toString(), map.get(SAMZA_VERSION).toString(),
        map.get(HOST).toString(),
        ((Number) map.get(TIME)).longValue(),
        ((Number) map.get(RESET_TIME)).longValue(),
        schemaVersion,
        portableJobFields);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricsHeader that = (MetricsHeader) o;
    return time == that.time && resetTime == that.resetTime && Objects.equals(jobName, that.jobName) && Objects.equals(
        jobId, that.jobId) && Objects.equals(containerName, that.containerName) && Objects.equals(
        execEnvironmentContainerId, that.execEnvironmentContainerId) && Objects.equals(samzaEpochId,
        that.samzaEpochId) && Objects.equals(source, that.source) && Objects.equals(version, that.version)
        && Objects.equals(samzaVersion, that.samzaVersion) && Objects.equals(host, that.host)
        && Objects.equals(schemaVersion, that.schemaVersion)
        && Objects.equals(portableJobFields, that.portableJobFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobName, jobId, containerName, execEnvironmentContainerId, samzaEpochId, source,
        version, samzaVersion, host, time, resetTime, schemaVersion, portableJobFields);
  }

  @Override
  public String toString() {
    return "MetricsHeader{" + "jobName='" + jobName + '\'' + ", jobId='" + jobId + '\'' + ", containerName='"
        + containerName + '\'' + ", execEnvironmentContainerId='" + execEnvironmentContainerId + '\''
        + ", samzaEpochId=" + samzaEpochId + ", source='" + source + '\'' + ", version='" + version + '\''
        + ", samzaVersion='" + samzaVersion + '\'' + ", host='" + host + '\'' + ", time=" + time + ", resetTime="
        + resetTime + ", schemaVersion=" + schemaVersion + ", portableJobFields=" + portableJobFields + '}';
  }

  public static class PortableJobFields {

    // Whether the job is using compute isolation
    public final boolean isPortableJob;

    // Can be set to worker or runner - for jobs using compute isolation
    public final ProcessType processType;

    // Can be used to identify a worker if there are multiple workers - for jobs using compute isolation
    public final String portableJobProcessId;

    // Enum to identify the type of process for jobs using compute isolation (portable mode).
    public enum ProcessType {

      // The runner process runs framework code. There can only be one runner per container.
      Runner,

      // Worker process runs user code. There can be one or more of workers in a single container.
      Worker
    }

    private PortableJobFields() {
      this(false, ProcessType.Runner, PORTABLE_JOB_RUNNER_PROCESS_ID);
    }

    public PortableJobFields(boolean isPortableJob, ProcessType processType,
        String portableJobProcessId) {
      this.isPortableJob = isPortableJob;
      this.processType = processType;
      this.portableJobProcessId = portableJobProcessId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isPortableJob, processType, portableJobProcessId);
    }

    @Override
    public String toString() {
      return "PortableJobFields{"
          + ", isPortableJob='" + isPortableJob + '\''
          + ", processType='" + processType + '\''
          + ", portableJobProcessId='" + portableJobProcessId + '\''
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PortableJobFields that = (PortableJobFields) o;
      return this.isPortableJob == that.isPortableJob
          && this.processType == that.processType
          && Objects.equals(this.portableJobProcessId, that.portableJobProcessId);
    }
  }
}