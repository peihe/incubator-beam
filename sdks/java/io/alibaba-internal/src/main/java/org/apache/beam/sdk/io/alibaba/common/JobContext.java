/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.alibaba.common;

public class JobContext {

  public static String jobName = null;
  public static String project = null;
  public static boolean debug = false;

  public static String getJobName() {
    return jobName;
  }

  public synchronized static void setJobName(String name) {
    jobName = name;
  }

  public static String getProject() {
    return project;
  }

  public synchronized static void setProject(String name) {
    project = name;
  }

  public static boolean isDebug() {
    return debug;
  }

  public synchronized static void setDebug(boolean isDebug) {
    debug = isDebug;
  }
}
