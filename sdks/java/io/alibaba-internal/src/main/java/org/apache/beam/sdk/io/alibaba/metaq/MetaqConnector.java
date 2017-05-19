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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.taobao.metaq.client.MetaProducer;
import com.taobao.metaq.client.MetaPullConsumer;

public class MetaqConnector {
  private static final Logger logger = LoggerFactory.getLogger(MetaqConnector.class);

  private static final int DEFAULT_RETRY_SIZE = 10;
  private String topicName;
  private String groupName;
  private String tag;
  private MetaPullConsumer messageConsumer;
  private List<MessageQueue> partitions;
  private String nameSerAdd = null;
  public int metaConsumerSize = 1024 * 1024;
  private MetaProducer producer = null;

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public MetaqConnector(String groupName, String topic, String serAdd, String tag) {
    this.groupName = groupName;
    this.topicName = topic;
    this.nameSerAdd = serAdd;
    this.tag = tag;
  }

  public MetaProducer getProducer() throws MQClientException {
    if (producer == null) {
      this.producer = MetaqProducerInstance.getProducer(groupName, nameSerAdd);
    }
    return this.producer;
  }

  public void setProducer(MetaProducer producer) {
    this.producer = producer;
  }

  public void closeProducer() {
    if (producer != null) {
      producer.shutdown();
      MetaqProducerInstance.closeProducer(groupName, nameSerAdd);
    }
  }

  public void connect() throws MQClientException {
    if (this.messageConsumer == null) {
      this.messageConsumer = MetaqConsumerInstance.getConsumer(this.topicName, this.groupName, this.nameSerAdd);
    }
  }

  public List<MessageQueue> getPartition() throws MQClientException {
    if (this.messageConsumer == null) {
      this.connect();
    }
    Set<MessageQueue> messageQueueSet = this.messageConsumer.fetchSubscribeMessageQueues(this.topicName);
    Iterator<MessageQueue> msgQueueIterator = messageQueueSet.iterator();
    List<MessageQueue> msgQueueList = new ArrayList<MessageQueue>();
    while (msgQueueIterator.hasNext()) {
      MessageQueue msgQueue = msgQueueIterator.next();
      msgQueueList.add(msgQueue);
    }
    Collections.sort(msgQueueList, new SortMessageQueue());
    return msgQueueList;
  }

  class SortMessageQueue implements Comparator<MessageQueue> {
    @Override
    public int compare(MessageQueue o1, MessageQueue o2) {
      return o1.compareTo(o2);
    }
  }

  public int getPartitionAmount() throws MQClientException {
    if (partitions == null) {
      return this.getPartition().size();
    } else {
      return this.partitions.size();
    }
  }

  public static String getPartitionName(MessageQueue queue) {
    return queue.getTopic() + "_" + queue.getBrokerName() + "_" + queue.getQueueId();
  }

  public List<String> getPartitionNameList() throws Exception {
    List<String> nameList = new LinkedList<String>();
    List<MessageQueue> partitions = this.getPartition();
    if (partitions != null && !partitions.isEmpty()) {
      for (MessageQueue queue : partitions) {
        nameList.add(getPartitionName(queue));
      }
    } else {
      throw new Exception("partitions is null.");
    }
    return nameList;
  }

  public MetaActiveReader getReader(String partitionName, Date startTime) throws Exception {
    if (partitions == null || partitions.isEmpty()) {
      this.partitions = this.getPartition();
    }
    if (partitions != null && !partitions.isEmpty()) {
      for (MessageQueue queue : partitions) {
        String name = getPartitionName(queue);
        if (partitionName.equals(name)) {
          return new MetaActiveReader(this.messageConsumer, this.topicName, queue, tag, startTime.getTime());
        }
      }
    }

    throw new Exception("no partition named[" + partitionName + "]");
  }

  /**
   * 主动模式下保存消费的offset
   */
  public class MetaActiveReader {

    private Logger logger = LoggerFactory.getLogger(MetaActiveReader.class);

    public String topicName;
    public MetaPullConsumer consumer = null;
    public MessageQueue partition = null;
    public long startOffset;
    public long offset;
    private PullResult pullResult;
    private long startTime;

    private String tag;

    MetaActiveReader(MetaPullConsumer consumer, String topicName, MessageQueue queue, String tag) {
      this.consumer = consumer;
      this.topicName = topicName;
      this.partition = queue;
      this.tag = tag;
    }

    /**
     * 如果指定的offset不存在，则从startTime开始读数据
     *
     * @throws MQClientException
     */
    MetaActiveReader(MetaPullConsumer consumer, String topicName, MessageQueue partition, String tag, long startTime)
        throws MQClientException {
      this(consumer, topicName, partition, tag);
      this.startTime = startTime;
      this.offset = consumer.searchOffset(partition, this.startTime);
      this.startOffset = this.offset;
      logger.info("partion id is " + partition.getBrokerName() + "-" + partition.getQueueId() + " start time is"
          + this.startTime + " start offset " + this.startOffset);
    }

    public long getOffset() {
      return this.offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public long getStartTime() {
      return startTime;
    }

    public Iterator<MessageExt> getMessages(int fetchSize) throws Exception {
      // 如果没有数据就立刻返回
      int retry = 1;
      while (true) {
        try {
          pullResult = consumer.pull(partition, this.tag, this.offset, fetchSize);
          break;
        } catch (Exception e) {
          logger.warn("fetch data from metaq fail, retry[" + retry + "]", e);
          retry++;
          if (retry > DEFAULT_RETRY_SIZE) {
            throw e;
          }
          Thread.sleep(2000);
        }
      }

      List<MessageExt> msgExtList = null;
      Iterator<MessageExt> msgExtIterator = null;
      this.offset = pullResult.getNextBeginOffset();
      switch (pullResult.getPullStatus()) {
        case FOUND:
          msgExtList = pullResult.getMsgFoundList();
          msgExtIterator = msgExtList.iterator();
          break;
        case NO_MATCHED_MSG:
          break;
        case NO_NEW_MSG:
          break;
        case OFFSET_ILLEGAL:
          break;
        default:
          break;
      }

      return msgExtIterator;
    }

    public MessageQueue getPartition() {
      return partition;
    }

    public void setPartition(MessageQueue partition) {
      this.partition = partition;
    }

    public long getFirstStartOffset() {
      return this.startOffset;
    }
  }
}
