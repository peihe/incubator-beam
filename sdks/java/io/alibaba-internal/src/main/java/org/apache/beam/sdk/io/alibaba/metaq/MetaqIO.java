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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.io.alibaba.common.JobContext;
import org.apache.beam.sdk.io.alibaba.common.Utils;
import org.apache.beam.sdk.io.alibaba.metaq.MetaqConnector.MetaActiveReader;
import org.apache.beam.sdk.io.alibaba.common.TTBlockParser;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.metaq.client.MetaProducer;

/**
 * 
 * @author zhigang.lzg
 * @date 2017年4月25日
 */
public class MetaqIO {

  private static final Logger LOG = LoggerFactory.getLogger(MetaqIO.class);

  public static Read<List<String>> read(String groupName, String topicName) {
    return new Read<>(groupName, topicName, ListCoder.of(StringUtf8Coder.of()), new TTBlockParser());
  }

  public static <T> Read<T> read(String groupName, String topicName, Coder<T> coder,
      SerializableFunction<byte[], List<T>> dataFn) {
    return new Read<>(groupName, topicName, coder, dataFn);
  }

  public static <T> Write<T> write(String groupName, String topicName, Coder<T> coder) {
    return new Write<>(groupName, topicName, coder);
  }

  //////////////////////// Source Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  public static class Read<T> extends PTransform<PBegin, PCollection<MetaqRecord<T>>> {

    private String groupName;
    private String topicName;
    private String nameSerAdd;
    private String tag;
    private Date startTime;
    private String partition;
    private Coder<T> coder;
    private SerializableFunction<byte[], List<T>> dataFn;
    private SerializableFunction<MetaqRecord<T>, Instant> watermarkFn;
    private SerializableFunction<MetaqRecord<T>, Instant> timestampFn;
    private SerializableFunction<Map<String, String>, Map<String, String>> metaqPropFilter;
    private int retryTime;

    public Read() {
    }

    Read(String groupName, String topicName, Coder<T> coder, SerializableFunction<byte[], List<T>> dataFn) {
      this(groupName, topicName, null, null, coder, dataFn, new Date(), null, null, null, new DefaultPropFilter(), 10);
    }

    Read(String groupName, String topicName, String nameSerAdd, String tag, Coder<T> coder,
        SerializableFunction<byte[], List<T>> dataFn, Date startTime, String partition,
        SerializableFunction<MetaqRecord<T>, Instant> watermarkFn,
        SerializableFunction<MetaqRecord<T>, Instant> timestampFn,
        SerializableFunction<Map<String, String>, Map<String, String>> metaqPropFilter, int retryTime) {
      this.groupName = groupName;
      this.topicName = topicName;
      this.nameSerAdd = nameSerAdd;
      this.tag = tag;
      this.coder = coder;
      this.dataFn = dataFn;
      this.startTime = startTime;
      this.partition = partition;
      this.watermarkFn = watermarkFn;
      this.timestampFn = timestampFn;
      this.metaqPropFilter = metaqPropFilter;
      this.retryTime = retryTime;
    }

    public PCollection<MetaqRecord<T>> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<MetaqRecord<T>> unbounded = org.apache.beam.sdk.io.Read.from(makeSource());
      return input.getPipeline().apply(unbounded);
    }

    UnboundedSource<MetaqRecord<T>, MetaqCheckpointMark> makeSource() {
      return new UnboundedMetaqSource<>(this, -1);
    }

    public int getRetryTime() {
      return retryTime;
    }

    public void setRetryTime(int retryTime) {
      this.retryTime = retryTime;
    }

    public void setGroupName(String groupName) {
      this.groupName = groupName;
    }

    public void setTopicName(String topicName) {
      this.topicName = topicName;
    }

    public void setNameSerAdd(String nameSerAdd) {
      this.nameSerAdd = nameSerAdd;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public void setCoder(Coder<T> coder) {
      this.coder = coder;
    }

    public SerializableFunction<byte[], List<T>> getDataFn() {
      return dataFn;
    }

    public void setDataFn(SerializableFunction<byte[], List<T>> dataFn) {
      this.dataFn = dataFn;
    }

    /**
     * currData and currTTTimestamp to watermark.
     */
    public void setWatermarkFn(SerializableFunction<MetaqRecord<T>, Instant> watermarkFn) {
      this.watermarkFn = watermarkFn;
    }

    public Date getStartTime() {
      return startTime;
    }

    public void setStartTime(Date startTime) {
      this.startTime = startTime;
    }

    /**
     * currData to its timestamp.
     */
    public void setTimestampFn(SerializableFunction<MetaqRecord<T>, Instant> timestampFn) {
      this.timestampFn = timestampFn;
    }

    /**
     * transform metaq prop.
     */
    public void setTtPropFilter(SerializableFunction<Map<String, String>, Map<String, String>> metaqPropFilter) {
      this.metaqPropFilter = metaqPropFilter;
    }

    public String getGroupName() {
      return groupName;
    }

    public String getTopicName() {
      return topicName;
    }

    public String getNameSerAdd() {
      return nameSerAdd;
    }

    public String getTag() {
      return tag;
    }

    public String getPartition() {
      return partition;
    }

    public Coder<T> getCoder() {
      return coder;
    }

    public SerializableFunction<MetaqRecord<T>, Instant> getWatermarkFn() {
      return watermarkFn;
    }

    public SerializableFunction<MetaqRecord<T>, Instant> getTimestampFn() {
      return timestampFn;
    }

    public SerializableFunction<Map<String, String>, Map<String, String>> getMetaqPropFilter() {
      return metaqPropFilter;
    }

    private static class DefaultPropFilter implements SerializableFunction<Map<String, String>, Map<String, String>> {
      @Override
      public Map<String, String> apply(Map<String, String> input) {
        return Collections.emptyMap();
      }
    }
  }

  private static class UnboundedMetaqSource<T> extends UnboundedSource<MetaqRecord<T>, MetaqCheckpointMark> {
    private Read<T> spec;
    private final int id; // split id, mainly for debugging

    public UnboundedMetaqSource(Read<T> spec, int id) {
      this.spec = spec;
      this.id = id;
    }

    @Override
    public List<UnboundedMetaqSource<T>> split(int desiredNumSplits, PipelineOptions options)
        throws Exception {
      List<String> partitions = new ArrayList<>();

      // (a) fetch partitions
      // (b) sort by
      // (c) round-robin assign the partitions to splits
      MetaqConnector conn = new MetaqConnector(spec.getGroupName(), spec.getTopicName(), spec.getNameSerAdd(),
          spec.tag);

      partitions = conn.getPartitionNameList();

      Collections.sort(partitions);

      checkState(partitions.size() > 0,
          "Could not find any partitions. Please check metaq configuration and topic names");

      int numSplits = partitions.size();

      List<UnboundedMetaqSource<T>> result = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        result.add(new UnboundedMetaqSource<>(new Read<>(spec.getGroupName(), spec.getTopicName(), spec.getNameSerAdd(),
            spec.getTag(), spec.getCoder(), spec.getDataFn(), spec.getStartTime(), partitions.get(i),
            spec.getWatermarkFn(), spec.getTimestampFn(), spec.getMetaqPropFilter(), spec.getRetryTime()), i));
      }

      return result;
    }

    @Override
    public UnboundedMetaqReader<T> createReader(PipelineOptions options, MetaqCheckpointMark checkpointMark) {
      checkNotNull(spec.getPartition());
      return new UnboundedMetaqReader<>(this, checkpointMark);
    }

    @Override
    public Coder<MetaqCheckpointMark> getCheckpointMarkCoder() {
      return new MetaqCheckpointMarkCoder();
    }

    @Override
    public boolean requiresDeduping() {
      // metaq records are ordered with in partitions. In addition checkpoint
      // guarantees
      // records are not consumed twice.
      return false;
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<MetaqRecord<T>> getDefaultOutputCoder() {
      return new MetaqRecordCoder<>(spec.getCoder());
    }
  }

  private static class UnboundedMetaqReader<T> extends UnboundedSource.UnboundedReader<MetaqRecord<T>> {

    private static Instant initialWatermark = new Instant(Long.MIN_VALUE);

    private final UnboundedMetaqSource<T> source;
    private final String name;
    private final String partition;
    private MetaActiveReader input;
    private String nowOffset;
    private MetaqRecord<T> curRecord;
    private Instant curTimestamp;
    private boolean hasStart;
    private MetaqConnector conn;
    private int index;

    public UnboundedMetaqReader(UnboundedMetaqSource<T> source, @Nullable MetaqCheckpointMark checkpointMark) {
      this.source = source;
      this.name = "Reader-" + source.id;
      partition = source.spec.getPartition();
      if (checkpointMark != null) {
        this.nowOffset = checkpointMark.getOffset();
      }
      hasStart = false;
    }

    @Override
    public boolean start() throws IOException {
      initInput();
      return advance();
    }

    private void initInput() throws IOException {
      Exception exception = null;
      for (int i = 0; i < source.spec.getRetryTime(); i++) {
        try {
          if (input != null) {
            return;
          }

          Read spec = source.spec;
          conn = new MetaqConnector(spec.getGroupName(), spec.getTopicName(), spec.getNameSerAdd(), spec.getTag());
          input = conn.getReader(partition, spec.getStartTime());
          return;
        } catch (Exception e) {
          exception = e;
        }
      }

      throw new RuntimeException(exception);
    }

    @Override
    public boolean advance() throws IOException {
      try {
        if (!hasStart) {
          if (nowOffset != null) {
            input.setOffset(Long.valueOf(nowOffset));
          }
          hasStart = true;
        }

        Iterator<MessageExt> ret = input.getMessages(1);
        if (ret == null || !ret.hasNext()) {
          return false;
        } else {
          MessageExt messageExt = ret.next();
          byte[] buffer = messageExt.getBody();
          Map<String, String> prop = new HashMap<String, String>();
          if (messageExt.getProperties() != null) {
            prop.putAll(messageExt.getProperties());
          }
          prop.put("timestamp", String.valueOf(messageExt.getStoreTimestamp()));

          int bytesSize = buffer.length;
          long metaqTime = getMetaqTimeStamp(prop);
          List<T> data = source.spec.getDataFn().apply(buffer);
          prop = source.spec.getMetaqPropFilter().apply(prop);
          curRecord = new MetaqRecord<>(partition, String.valueOf(messageExt.getQueueOffset()), prop, data, bytesSize,
              metaqTime);
          curTimestamp = (source.spec.getTimestampFn() == null) ? Instant.now()
              : source.spec.getTimestampFn().apply(curRecord);
          nowOffset = String.valueOf(messageExt.getQueueOffset() + 1);
          return true;
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    private long getMetaqTimeStamp(Map<String, String> prop) {
      if (prop != null && prop.containsKey("timestamp")) {
        return Long.valueOf(prop.get("timestamp"));
      } else {
        return 0;
      }
    }

    @Override
    public Instant getWatermark() {
      if (curRecord == null) {
        LOG.debug("{}: getWatermark() : no records have been read yet.", name);
        return initialWatermark;
      }

      return source.spec.getWatermarkFn() != null ? source.spec.getWatermarkFn().apply(curRecord) : curTimestamp;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new MetaqCheckpointMark(nowOffset);
    }

    @Override
    public UnboundedSource<MetaqRecord<T>, ?> getCurrentSource() {
      return source;
    }

    @Override
    public MetaqRecord<T> getCurrent() throws NoSuchElementException {
      return curRecord;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return curTimestamp;
    }

    @Override
    public long getSplitBacklogBytes() {
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }

    @Override
    public void close() throws IOException {
    }
  }

  //////////////////////// Sink Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    private String groupName;
    private String topicName;
    private String nameSerAdd;
    private String tag;
    private Coder<T> coder;
    private SerializableFunction<T, Map<String, String>> propFn;
    private int retryTime;

    public Write() {
    }

    public Write(String groupName, String topicName, Coder<T> coder) {
      this(groupName, topicName, null, null, coder, null, 10);
    }

    public Write(String groupName, String topicName, String nameSerAdd, String tag, Coder<T> coder,
        SerializableFunction<T, Map<String, String>> propFn, int retryTime) {
      this.groupName = groupName;
      this.topicName = topicName;
      this.nameSerAdd = nameSerAdd;
      this.tag = tag;
      this.coder = coder;
      this.propFn = propFn;
      this.retryTime = retryTime;
    }

    public String getGroupName() {
      return groupName;
    }

    public void setGroupName(String groupName) {
      this.groupName = groupName;
    }

    public String getTopicName() {
      return topicName;
    }

    public void setTopicName(String topicName) {
      this.topicName = topicName;
    }

    public String getNameSerAdd() {
      return nameSerAdd;
    }

    public void setNameSerAdd(String nameSerAdd) {
      this.nameSerAdd = nameSerAdd;
    }

    public String getTag() {
      return tag;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public Coder<T> getCoder() {
      return coder;
    }

    public void setCoder(Coder<T> coder) {
      this.coder = coder;
    }

    public SerializableFunction<T, Map<String, String>> getPropFn() {
      return propFn;
    }

    public void setPropFn(SerializableFunction<T, Map<String, String>> propFn) {
      this.propFn = propFn;
    }

    public int getRetryTime() {
      return retryTime;
    }

    public void setRetryTime(int retryTime) {
      this.retryTime = retryTime;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(new MetaqWriter<>(this)));
      return PDone.in(input.getPipeline());
    }

  }

  private static class MetaqWriter<T> extends DoFn<T, Void> {

    private Write<T> spec;
    private MetaqConnector conn;
    private MetaProducer metaqProducer;
    // private OutputMetrics outputMetrics;

    public MetaqWriter(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      try {
        conn = new MetaqConnector(spec.getGroupName(), spec.getTopicName(), spec.getNameSerAdd(), spec.getTag());
        metaqProducer = conn.getProducer();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
      /* TODO: figure out if the following code could be removed, or replaced with Beam metrics API.
      if (outputMetrics == null) {
        JobContext.setJobName(ctx.getPipelineOptions().getJobName());
        JobContext.setProject(AbstractProcessor.getBatchContext().project);
        outputMetrics = new OutputMetrics(AbstractProcessor.getBatchContext().component,
            String.valueOf(AbstractProcessor.getBatchContext().task));
      }
      outputMetrics.outputMessages.incr();
      outputMetrics.outputQps.incr();
      */
      
      T t = ctx.element();
      byte[] bytes = CoderUtils.encodeToByteArray(spec.getCoder(), t);
      Message message = newMessage(bytes, newProp(t));
      if (message != null) {
        message.setTopic(spec.getTopicName());
        if (spec.getTag() == null) {
          message.setTags("galaxy");
        } else {
          message.setTags(spec.getTag());
        }

        int retryTime = 0;
        boolean flag = true;
        do {
          try {
            flag = true;
            SendResult result = metaqProducer.send(message);
            if (!result.getSendStatus().equals(SendStatus.SEND_OK)) {
              LOG.warn("write to metaq result is " + result.getSendStatus().toString());
              flag = false;
            }
          } catch (Exception e) {
            flag = false;
            LOG.error("write to metaq error, " + Utils.getStackTrace(e));
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e1) {
              LOG.error("Thread sleep failed");
            }
          }
        } while ((!flag) && (retryTime++ < spec.getRetryTime()));

        if (flag) {
          return;
        } else {
          throw new Exception("write metaq failed, after write " + retryTime + " times");
        }
      }
    }

    private Map<String, String> newProp(T t) {
      if (spec.getPropFn() == null) {
        return Collections.emptyMap();
      } else {
        return spec.getPropFn().apply(t);
      }
    }

    private Message newMessage(byte[] bytes, Map<String, String> prop) {
      Message block = new Message();
      block.setBody(bytes);
      block.setWaitStoreMsgOK(true);
      for (Map.Entry<String, String> entry : prop.entrySet()) {
        block.putUserProperty(entry.getKey(), entry.getValue());
      }
      return block;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws IOException {
    }

    @Teardown
    public void teardown() {
      if (conn != null) {
        conn.closeProducer();
      }
    }
  }
}
