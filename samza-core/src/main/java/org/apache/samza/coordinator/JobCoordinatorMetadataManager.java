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
package org.apache.samza.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetJobCoordinatorMetadataMessage;
import org.apache.samza.job.JobCoordinatorMetadata;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to manage read and writes of {@link JobCoordinatorMetadata} to {@link MetadataStore}. It also provides
 * additional helper functionalities to generate {@link JobCoordinatorMetadata} and check for changes across runs.
 */
public class JobCoordinatorMetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobCoordinatorMetadataManager.class);

  static final String CONTAINER_ID_PROPERTY = "CONTAINER_ID";
  static final String CONTAINER_ID_DELIMITER = "_";

  private final JobCoordinatorMetadataManagerMetrics metrics;
  private final MetadataStore metadataStore;
  private final ObjectMapper metadataMapper = SamzaObjectMapper.getObjectMapper();
  private final Serde<String> valueSerde;
  private final ClusterType clusterType;

  public JobCoordinatorMetadataManager(MetadataStore metadataStore, ClusterType clusterType,
      MetricsRegistry metricsRegistry) {
    this(metadataStore, clusterType, metricsRegistry,
        new CoordinatorStreamValueSerde(SetJobCoordinatorMetadataMessage.TYPE));
  }

  @VisibleForTesting
  JobCoordinatorMetadataManager(MetadataStore metadataStore, ClusterType clusterType, MetricsRegistry metricsRegistry,
      Serde<String> valueSerde) {
    Preconditions.checkNotNull(clusterType, "Cluster type cannot be null");

    this.clusterType = clusterType;
    this.metadataStore = metadataStore;
    this.valueSerde = valueSerde;
    this.metrics = new JobCoordinatorMetadataManagerMetrics(metricsRegistry);
  }

  /**
   * Generates {@link JobCoordinatorMetadata} for the {@link JobCoordinator}.
   *
   * Epoch ID - It is generated by {@link #fetchEpochIdForJobCoordinator()}. Refer to the javadocs for more
   * details on how it is generated and the properties of the identifier.
   *
   * Config ID - A unique and reproducible identifier that is generated based on the input {@link Config}. It uses
   * a {@link Funnel} to use a subset of the input configuration to generate the identifier and as long as the subset
   * of the configuration remains same, the identifier is guaranteed to be same. For the list of config prefixes used
   * by the funnel refer to {@link ConfigHashFunnel}
   *
   * JobModel ID - A unique and reproducible identifier that is generated based on the input {@link JobModel}. It only
   * uses the {@link org.apache.samza.job.model.ContainerModel} within the {@linkplain JobModel} for generation. We
   * serialize the data into bytes and use those bytes to compute the identifier.
   *
   * In case of YARN, the epoch identifier is extracted from the application attempt and translates to applicationId
   * e.g. 1606797336059_0010
   * Both config and job model identifiers should a 32 bit integer.
   *
   * @param jobModel job model used for generating the metadata
   * @param config config used for generating the metadata
   *
   * @return the metadata for the job coordinator
   */
  public JobCoordinatorMetadata generateJobCoordinatorMetadata(JobModel jobModel, Config config) {
    try {
      int jobModelId = Hashing
          .crc32c()
          .hashBytes(SamzaObjectMapper.getObjectMapper().writeValueAsBytes(jobModel.getContainers()))
          .asInt();
      int configId = Hashing
          .crc32()
          .hashObject(config, new ConfigHashFunnel())
          .asInt();

      LOG.info("Generated job model id {} and config id {}", jobModelId, configId);
      return new JobCoordinatorMetadata(fetchEpochIdForJobCoordinator(), String.valueOf(configId),
          String.valueOf(jobModelId));
    } catch (Exception e) {
      metrics.incrementMetadataGenerationFailedCount();
      LOG.error("Failed to generate metadata for the current attempt due to ", e);
      throw new SamzaException("Failed to generate the metadata for the current attempt due to ", e);
    }
  }

  /**
   * Check for changes between the metadata passed as inputs. Metadata is considered changed if any of the attributes within
   * {@linkplain JobCoordinatorMetadata} changes.
   *
   * We intentionally check for each changes to help us track at this granularity. We want to use this information
   * to determine if complex handling is required to cater these changes instead of blindly restarting all the
   * containers upstream.
   *
   * @param newMetadata new metadata to be compared
   * @param previousMetadata previous metadata to be compared against
   *
   * @return true if metadata changed, false otherwise
   */
  public boolean checkForMetadataChanges(JobCoordinatorMetadata newMetadata, JobCoordinatorMetadata previousMetadata) {
    boolean changed = true;

    if (previousMetadata == null) {
      metrics.setNewDeployment(1);
    } else if (!previousMetadata.getEpochId().equals(newMetadata.getEpochId())) {
      metrics.setNewDeployment(1);
    } else if (!previousMetadata.getJobModelId().equals(newMetadata.getJobModelId())) {
      metrics.setJobModelChangedAcrossApplicationAttempt(1);
    } else if (!previousMetadata.getConfigId().equals(newMetadata.getConfigId())) {
      metrics.setConfigChangedAcrossApplicationAttempt(1);
    } else {
      changed = false;
      metrics.incrementApplicationAttemptCount();
    }

    if (changed) {
      LOG.info("Job coordinator metadata changed from: {} to: {}", previousMetadata, newMetadata);
    } else {
      LOG.info("Job coordinator metadata {} unchanged.", newMetadata);
    }

    return changed;
  }

  /**
   * Reads the {@link JobCoordinatorMetadata} from the metadata store. It fetches the metadata
   * associated with cluster type specified at the creation of the manager.
   *
   * @return job coordinator metadata
   */
  public JobCoordinatorMetadata readJobCoordinatorMetadata() {
    JobCoordinatorMetadata metadata = null;
    for (Map.Entry<String, byte[]> entry : metadataStore.all().entrySet()) {
      if (clusterType.name().equals(entry.getKey())) {
        try {
          String metadataString = valueSerde.fromBytes(entry.getValue());
          metadata = metadataMapper.readValue(metadataString, JobCoordinatorMetadata.class);
          break;
        } catch (Exception e) {
          metrics.incrementMetadataReadFailedCount();
          LOG.error("Failed to read job coordinator metadata due to ", e);
        }
      }
    }

    LOG.info("Fetched the job coordinator metadata for cluster {} as {}.", clusterType, metadata);
    return metadata;
  }

  /**
   * Persist the {@link JobCoordinatorMetadata} in metadata store. The job coordinator metadata is associated
   * with the cluster type specified at the creation of the manager.
   *
   * @param metadata metadata to be persisted
   *
   * @throws SamzaException in case of exception encountered during the writes to underlying metadata store
   */
  public void writeJobCoordinatorMetadata(JobCoordinatorMetadata metadata) {
    Preconditions.checkNotNull(metadata, "Job coordinator metadata cannot be null");

    try {
      String metadataValueString = metadataMapper.writeValueAsString(metadata);
      metadataStore.put(clusterType.name(), valueSerde.toBytes(metadataValueString));
      LOG.info("Successfully written job coordinator metadata: {} for cluster {}.", metadata, clusterType);
    } catch (Exception e) {
      metrics.incrementMetadataWriteFailedCount();
      LOG.error("Failed to write the job coordinator metadata to metadata store due to ", e);
      throw new SamzaException("Failed to write the job coordinator metadata.", e);
    }
  }

  /**
   * Generate the epoch id using the execution container id that is passed through system environment. This isn't ideal
   * way of generating this ID and we will need some contract between the underlying cluster manager and samza engine
   * around what the epoch ID should be like and what is needed to generate is across different cluster offerings.
   * Due to unknowns defined above, we leave it as is and keep it simple for now. It is favorable to keep it this way
   * instead of introducing a loosely defined interface/API and marking it unstable.
   *
   * The properties of the epoch identifier are as follows
   *  1. Unique across applications in the cluster
   *  2. Remains unchanged within a single deployment lifecycle
   *  3. Remains unchanged across application attempt within a single deployment lifecycle
   *  4. Changes across deployment lifecycle
   *
   *  Note: The above properties is something we want keep intact when extracting this into a well defined interface
   *  or contract for YARN AM HA to work.
   *  The format and property used to generate ID is specific to YARN and the specific format of the container name
   *  is a public contract by YARN which is likely to remain backward compatible.
   *
   * @return an identifier associated with the job coordinator satisfying the above properties
   */
  @VisibleForTesting
  String fetchEpochIdForJobCoordinator() {
    String[] containerIdParts = getEnvProperty(CONTAINER_ID_PROPERTY).split(CONTAINER_ID_DELIMITER);
    return containerIdParts[1] + CONTAINER_ID_DELIMITER + containerIdParts[2];
  }

  @VisibleForTesting
  String getEnvProperty(String propertyName) {
    return System.getenv(propertyName);
  }

  @VisibleForTesting
  JobCoordinatorMetadataManagerMetrics getMetrics() {
    return metrics;
  }

  /**
   * A helper class to generate hash for the {@link Config} based on with a subset of configuration.
   * The subset of configuration used are configurations that prefix match the allowed prefixes.
   */
  private static class ConfigHashFunnel implements Funnel<Config> {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigHashFunnel.class);
    // using sorted set to ensure the hash computation on configurations is reproducible and deterministic
    private static final SortedSet<String> ALLOWED_PREFIXES = ImmutableSortedSet.of("job.autosizing");
    @Override
    public void funnel(Config from, PrimitiveSink into) {
      SortedMap<String, String> map = new TreeMap<>();

      ALLOWED_PREFIXES.forEach(prefix -> map.putAll(from.subset(prefix, false)));
      LOG.info("Using the config {} to generate hash", map);
      map.forEach((key, value) -> {
        into.putUnencodedChars(key);
        into.putUnencodedChars(value);
      });
    }
  }

  /**
   * Type of the cluster deployment associated with the {@link JobCoordinatorMetadataManager}
   */
  public enum ClusterType {
    YARN
  }

  /**
   * A container class to hold all the metrics related to {@link JobCoordinatorMetadataManager}.
   */
  static class JobCoordinatorMetadataManagerMetrics {
    private static final String APPLICATION_ATTEMPT_COUNT = "application-attempt-count";
    private static final String GROUP = "JobCoordinatorMetadataManager";
    private static final String JOB_MODEL_CHANGED = "job-model-changed";
    private static final String CONFIG_CHANGED = "config-changed";
    private static final String METADATA_GENERATION_FAILED_COUNT = "metadata-generation-failed-count";
    private static final String METADATA_READ_FAILED_COUNT = "metadata-read-failed-count";
    private static final String METADATA_WRITE_FAILED_COUNT = "metadata-write-failed-count";
    private static final String NEW_DEPLOYMENT = "new-deployment";

    private final Gauge<Integer> applicationAttemptCount;
    private final Gauge<Integer> metadataGenerationFailedCount;
    private final Gauge<Integer> metadataReadFailedCount;
    private final Gauge<Integer> metadataWriteFailedCount;
    private final Gauge<Integer> jobModelChangedAcrossApplicationAttempt;
    private final Gauge<Integer> configChangedAcrossApplicationAttempt;
    private final Gauge<Integer> newDeployment;

    public JobCoordinatorMetadataManagerMetrics(MetricsRegistry registry) {
      applicationAttemptCount = registry.newGauge(GROUP, APPLICATION_ATTEMPT_COUNT, 0);
      configChangedAcrossApplicationAttempt =
          registry.newGauge(GROUP, CONFIG_CHANGED, 0);
      jobModelChangedAcrossApplicationAttempt =
          registry.newGauge(GROUP, JOB_MODEL_CHANGED, 0);
      metadataGenerationFailedCount = registry.newGauge(GROUP,
          METADATA_GENERATION_FAILED_COUNT, 0);
      metadataReadFailedCount = registry.newGauge(GROUP, METADATA_READ_FAILED_COUNT, 0);
      metadataWriteFailedCount = registry.newGauge(GROUP, METADATA_WRITE_FAILED_COUNT, 0);
      newDeployment = registry.newGauge(GROUP, NEW_DEPLOYMENT, 0);
    }

    @VisibleForTesting
    Gauge<Integer> getApplicationAttemptCount() {
      return applicationAttemptCount;
    }

    @VisibleForTesting
    Gauge<Integer> getMetadataGenerationFailedCount() {
      return metadataGenerationFailedCount;
    }

    @VisibleForTesting
    Gauge<Integer> getMetadataReadFailedCount() {
      return metadataReadFailedCount;
    }

    @VisibleForTesting
    Gauge<Integer> getMetadataWriteFailedCount() {
      return metadataWriteFailedCount;
    }

    @VisibleForTesting
    Gauge<Integer> getJobModelChangedAcrossApplicationAttempt() {
      return jobModelChangedAcrossApplicationAttempt;
    }

    @VisibleForTesting
    Gauge<Integer> getConfigChangedAcrossApplicationAttempt() {
      return configChangedAcrossApplicationAttempt;
    }

    @VisibleForTesting
    Gauge<Integer> getNewDeployment() {
      return newDeployment;
    }

    void incrementApplicationAttemptCount() {
      applicationAttemptCount.set(applicationAttemptCount.getValue() + 1);
    }

    void incrementMetadataGenerationFailedCount() {
      metadataGenerationFailedCount.set(metadataGenerationFailedCount.getValue() + 1);
    }

    void incrementMetadataReadFailedCount() {
      metadataReadFailedCount.set(metadataReadFailedCount.getValue() + 1);
    }

    void incrementMetadataWriteFailedCount() {
      metadataWriteFailedCount.set(metadataWriteFailedCount.getValue() + 1);
    }

    void setConfigChangedAcrossApplicationAttempt(int value) {
      configChangedAcrossApplicationAttempt.set(value);
    }

    void setJobModelChangedAcrossApplicationAttempt(int value) {
      jobModelChangedAcrossApplicationAttempt.set(value);
    }

    void setNewDeployment(int value) {
      newDeployment.set(value);
    }
  }
}
