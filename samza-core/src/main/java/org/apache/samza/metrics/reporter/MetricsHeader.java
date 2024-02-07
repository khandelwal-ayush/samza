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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
  private static final String IS_PORTABLE_JOB_LABEL = "is-portable-job";
  private static final String PORTABLE_JOB_PROCESS_ID_LABEL = "portable-job-process-id";
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

  // Whether the samza job is portable. This is used for portable jobs. This is optional for backwards compatibility.
  private final Optional<Boolean> isPortableJob;

  // Process ID for portable job. This is used for portable jobs. This is optional for backwards compatibility.
  private final Optional<String> portableJobProcessId;

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
      long resetTime, Optional<Boolean> isPortableJob, Optional<String> portableJobProcessId) {
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
    this.isPortableJob = isPortableJob;
    this.portableJobProcessId = portableJobProcessId;
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
    this.isPortableJob.ifPresent(isPortableJob -> map.put(IS_PORTABLE_JOB_LABEL, isPortableJob));
    this.portableJobProcessId.ifPresent(portableJobProcessId -> map.put(PORTABLE_JOB_PROCESS_ID_LABEL, portableJobProcessId));
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

  public Optional<Boolean> isPortableJob() { return isPortableJob; }

  public Optional<String> getPortableJobProcessId() { return portableJobProcessId; }

  public static MetricsHeader fromMap(Map<String, Object> map) {

    return new MetricsHeader(map.get(JOB_NAME).toString(), map.get(JOB_ID).toString(),
        map.get(CONTAINER_NAME).toString(), map.get(EXEC_ENV_CONTAINER_ID).toString(),
        // need to check existence for backwards compatibility with initial version of this class
        Optional.ofNullable(map.get(SAMZA_EPOCH_ID)).map(Object::toString), map.get(SOURCE).toString(),
        map.get(VERSION).toString(), map.get(SAMZA_VERSION).toString(),
        map.get(HOST).toString(),
        ((Number) map.get(TIME)).longValue(),
        ((Number) map.get(RESET_TIME)).longValue(),
        // need to check existence for backwards compatibility with initial version of this class
        Optional.ofNullable(map.get(IS_PORTABLE_JOB_LABEL)).map(Object::toString).map(Boolean::parseBoolean),
        // need to check existence for backwards compatibility with initial version of this class
        Optional.ofNullable(map.get(PORTABLE_JOB_PROCESS_ID_LABEL)).map(Object::toString));
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
        && Objects.equals(isPortableJob, that.isPortableJob)
        && Objects.equals(portableJobProcessId, that.portableJobProcessId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobName, jobId, containerName, execEnvironmentContainerId, samzaEpochId, source,
        version, samzaVersion, host, time, resetTime, isPortableJob, portableJobProcessId);
  }

  @Override
  public String toString() {
    return "MetricsHeader{" + "jobName='" + jobName + '\'' + ", jobId='" + jobId + '\'' + ", containerName='"
        + containerName + '\'' + ", execEnvironmentContainerId='" + execEnvironmentContainerId + '\''
        + ", samzaEpochId=" + samzaEpochId + ", source='" + source + '\'' + ", version='" + version + '\''
        + ", samzaVersion='" + samzaVersion + '\'' + ", host='" + host + '\'' + ", time=" + time + ", resetTime="
        + resetTime + ", isPortableJob=" + isPortableJob + ", portableJobProcessId=" + portableJobProcessId + '}';
  }
}