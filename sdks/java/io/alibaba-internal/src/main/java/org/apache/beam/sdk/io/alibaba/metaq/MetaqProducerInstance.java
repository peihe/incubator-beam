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
package org.apache.beam.sdk.io.alibaba.metaq;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.taobao.metaq.client.MetaProducer;

public class MetaqProducerInstance {
  public static ConcurrentHashMap<String, MetaProducer> producerMap = new ConcurrentHashMap<String, MetaProducer>();

  public static synchronized MetaProducer getProducer(String groupName, String nameSerAdd) throws MQClientException {
    String mapKey = groupName;
    if (nameSerAdd != null) {
      mapKey = mapKey + "||" + nameSerAdd;
    }

    if (producerMap.containsKey(mapKey)) {
      return producerMap.get(mapKey);
    } else {
      MetaProducer producer = new MetaProducer(groupName);

      RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
      String runtimeName = runtime.getName(); // format: "pid@hostname"

      String instanceName = runtimeName + "||" + groupName;
      producer.setInstanceName(instanceName);
      producerMap.put(mapKey, producer);

      if (nameSerAdd != null) {
        producer.setNamesrvAddr(nameSerAdd);
      }
      producer.start();
      return producer;
    }
  }

  public static void closeProducer(String groupName, String nameSerAdd) {
    String mapKey = groupName;
    if (nameSerAdd != null) {
      mapKey = mapKey + "||" + nameSerAdd;
    }
    if (producerMap.containsKey(mapKey)) {
      producerMap.remove(mapKey);
    }
  }
}
