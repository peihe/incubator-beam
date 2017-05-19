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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.alibaba.common.CountableValue;

/**
 * 
 * @author zhigang.lzg
 * @date 2017年4月26日
 * @param <T>
 */
public class MetaqRecord<T> implements Serializable, CountableValue {
  private static final long serialVersionUID = 1L;

  private final String partition;
  private final String offset;
  private final Map<String, String> properties;
  private final List<T> data; // metaq data
  private int bytesSize;
  private long metaqTime;

  public MetaqRecord(String partition, String offset, Map<String, String> properties, List<T> data, int bytesSize,
      long metaqTime) {
    this.partition = partition;
    this.offset = offset;
    this.properties = properties;
    this.data = data;
    this.bytesSize = bytesSize;
    this.metaqTime = metaqTime;
  }

  public String getPartition() {
    return partition;
  }

  public String getOffset() {
    return offset;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public List<T> getData() {
    return data;
  }

  @Override
  public String toString() {
    return "MetaqRecord{" + "partition='" + partition + '\'' + ", offset='" + offset + '\'' + ", properties="
        + properties + ", data=" + data + '}';
  }

  @Override
  public int getCount() {
    return data.size();
  }

  @Override
  public int getBytesSize() {
    return bytesSize;
  }

  @Override
  public long getDataTimeStamp() {
    return metaqTime;
  }
}
