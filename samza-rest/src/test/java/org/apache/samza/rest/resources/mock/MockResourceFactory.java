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
package org.apache.samza.rest.resources.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.rest.proxy.task.TaskResourceConfig;
import org.apache.samza.rest.resources.JobsResource;
import org.apache.samza.rest.resources.JobsResourceConfig;
import org.apache.samza.rest.resources.ResourceFactory;
import org.apache.samza.rest.resources.TasksResource;


public class MockResourceFactory implements ResourceFactory {

  @Override
  public List<? extends Object> getResourceInstances(Config config) {
    List<Object> resources = new ArrayList<>();
    if (config.containsKey(JobsResourceConfig.CONFIG_JOB_PROXY_FACTORY)) {
      resources.add(new JobsResource(new JobsResourceConfig(config)));
    }
    if (config.containsKey(TaskResourceConfig.CONFIG_TASK_PROXY_FACTORY)) {
      resources.add(new TasksResource(new TaskResourceConfig(config)));
    }
    return resources;
  }
}
