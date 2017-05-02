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
package com.alibaba.jstorm.beam.translation.runtime;

import com.alibaba.jstorm.beam.translation.util.CommonInstance;
import com.alibaba.jstorm.cache.IKvStore;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.cache.KvStoreManagerFactory;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.window.Watermark;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import com.alibaba.jstorm.beam.StormPipelineOptions;
import com.alibaba.jstorm.beam.util.SerializedPipelineOptions;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Spout implementation that wraps a Beam UnboundedSource
 */
public class UnboundedSourceSpout extends AdaptorBasicSpout {
    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceSpout.class);

    private final String description;
    private final UnboundedSource source;
    private final SerializedPipelineOptions serializedOptions;
    private final TupleTag<?> outputTag;

    private transient StormPipelineOptions pipelineOptions;
    private transient UnboundedSource.UnboundedReader reader;
    private transient SpoutOutputCollector collector;

    private transient boolean hasNextRecord;
    private AtomicBoolean activated = new AtomicBoolean();

    public UnboundedSourceSpout(
            String description,
            UnboundedSource source,
            StormPipelineOptions options,
            TupleTag<?> outputTag) {
        this.description = checkNotNull(description, "description");
        this.source = checkNotNull(source, "source");
        this.serializedOptions = new SerializedPipelineOptions(checkNotNull(options, "options"));
        this.outputTag = checkNotNull(outputTag, "outputTag");
    }

    @Override
    public synchronized void close() {
        try {
            this.reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {
        activated.set(true);
        
    }

    @Override
    public void deactivate() {
        activated.set(false);
    }

    @Override
    public void ack(Object msgId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fail(Object msgId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            this.pipelineOptions = this.serializedOptions.getPipelineOptions().as(StormPipelineOptions.class);

            createSourceReader(null);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create unbounded reader.", e);
        }
    }

    public void createSourceReader(UnboundedSource.CheckpointMark checkpointMark) throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = this.source.createReader(this.pipelineOptions, checkpointMark);
        hasNextRecord = this.reader.start();
    }

    @Override
    public synchronized void nextTuple() {
        if (!activated.get()) {
            return;
        }
        try {
            if (!hasNextRecord) {
                hasNextRecord = reader.advance();
            }

            while (hasNextRecord && activated.get()) {
                Object value = reader.getCurrent();
                Instant timestamp = reader.getCurrentTimestamp();

                WindowedValue wv = WindowedValue.of(value, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
                if (keyedEmit(outputTag.getId())) {
                    KV kv = (KV) wv.getValue();
                    // Convert WindowedValue<KV> to <K, WindowedValue<V>>
                    collector.emit(outputTag.getId(), new Values(kv.getKey(), wv.withValue(kv.getValue())));
                } else {
                    collector.emit(outputTag.getId(), new Values(wv));
                }

                // move to next record
                hasNextRecord = reader.advance();
            }

            Instant waterMark = reader.getWatermark();
            if (waterMark != null) {
                collector.flush();
                collector.emit(CommonInstance.BEAM_WATERMARK_STREAM_ID, new Values(new Watermark(waterMark.getMillis())));
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception reading values from source.", e);
        }
    }

    public UnboundedSource getUnboundedSource() {
        return source;
    }

    public UnboundedSource.UnboundedReader getUnboundedSourceReader() {
        return reader;
    }

    @Override
    public String toString() {
        return description;
    }
}
