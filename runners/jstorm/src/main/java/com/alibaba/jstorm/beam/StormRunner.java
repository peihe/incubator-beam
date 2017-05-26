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
package com.alibaba.jstorm.beam;

import static com.google.common.base.Preconditions.checkNotNull;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.jstorm.beam.translation.StormPipelineTranslator;
import com.alibaba.jstorm.beam.translation.TranslationContext;
import com.alibaba.jstorm.beam.translation.runtime.AbstractComponent;
import com.alibaba.jstorm.beam.translation.runtime.AdaptorBasicBolt;
import com.alibaba.jstorm.beam.translation.runtime.AdaptorBasicSpout;
import com.alibaba.jstorm.beam.translation.runtime.ExecutorsBolt;
import com.alibaba.jstorm.beam.translation.runtime.TxExecutorsBolt;
import com.alibaba.jstorm.beam.translation.runtime.TxUnboundedSourceSpout;
import com.alibaba.jstorm.beam.translation.runtime.UnboundedSourceSpout;
import com.alibaba.jstorm.beam.translation.translator.Stream;
import com.alibaba.jstorm.beam.translation.util.CommonInstance;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import com.alibaba.jstorm.utils.JStormUtils;

import java.io.IOException;
import java.util.HashMap;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point into the Storm Runner.
 * 
 * After reading the user defined pipeline, Beam will invoke the run() method with a representation of the pipeline.
 */
public class StormRunner extends PipelineRunner<StormRunner.StormPipelineResult> {
    private static final Logger LOG = LoggerFactory.getLogger(StormRunner.class);

    private StormPipelineOptions options;

    public StormRunner(StormPipelineOptions options) {
        this.options = options;
    }

    public static StormRunner fromOptions(PipelineOptions options) {
        StormPipelineOptions pipelineOptions = PipelineOptionsValidator.validate(StormPipelineOptions.class, options);
        return new StormRunner(pipelineOptions);
    }

    /**
     * convert pipeline options to storm configuration format
     * @param options
     * @return
     */
    private Config convertPipelineOptionsToConfig(StormPipelineOptions options) {
        Config config = new Config();
        if (options.getLocalMode())
            config.put(Config.STORM_CLUSTER_MODE, "local");
        else
            config.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        Config.setNumWorkers(config, options.getWorkerNumber());

        config.putAll(options.getTopologyConfig());

        return config;
    }

    @Override
    public StormPipelineResult run(Pipeline pipeline) {
        LOG.info("Running pipeline...");
        TranslationContext context = new TranslationContext(this.options);
        StormPipelineTranslator transformer = new StormPipelineTranslator(context);
        transformer.translate(pipeline);
        LOG.info("UserGraphContext=\n{}", context.getUserGraphContext());
        LOG.info("ExecutionGraphContext=\n{}", context.getExecutionGraphContext());

        for (Stream stream : context.getExecutionGraphContext().getStreams()) {
            LOG.info(stream.getProducer().getComponentId() + " --> " + stream.getConsumer().getComponentId());
        }

        String topologyName = options.getJobName();
        Config config = convertPipelineOptionsToConfig(options);

        return runTopology(
                topologyName,
                getTopology(options, context.getExecutionGraphContext()),
                config);
    }

    private StormPipelineResult runTopology(String topologyName, StormTopology topology, Config config) {
        try {
            if (StormConfig.local_mode(config)) {
                LocalCluster localCluster = LocalCluster.getInstance();
                localCluster.submitTopology(topologyName, config, topology);
                return new LocalStormPipelineResult(
                        topologyName, config, localCluster, options.getLocalModeExecuteTime());
            } else {
                StormSubmitter.submitTopology(topologyName, config, topology);
                return null;
            }
        } catch (Exception e) {
            LOG.warn("Fail to submit topology", e);
            throw new RuntimeException("Fail to submit topology", e);
        }
    }

    public static abstract class StormPipelineResult implements PipelineResult {

        private final String topologyName;
        private final Config config;

        StormPipelineResult(String topologyName, Config config) {
            this.config = checkNotNull(config, "config");
            this.topologyName = checkNotNull(topologyName, "topologyName");
        }

        public State getState() {
            return null;
        }

        public Config getConfig() {
            return config;
        }

        public String getTopologyName() {
            return topologyName;
        }
    }

    public static class LocalStormPipelineResult extends StormPipelineResult {

        private LocalCluster localCluster;
        private long localModeExecuteTimeSecs;

        LocalStormPipelineResult(
                String topologyName,
                Config config,
                LocalCluster localCluster,
                long localModeExecuteTimeSecs) {
            super(topologyName, config);
            this.localCluster = checkNotNull(localCluster, "localCluster");
        }

        @Override
        public State cancel() throws IOException {
            localCluster.deactivate(getTopologyName());
            localCluster.killTopology(getTopologyName());
            localCluster.shutdown();
            JStormUtils.sleepMs(1000);
            return State.CANCELLED;
        }

        @Override
        public State waitUntilFinish(Duration duration) {
            return waitUntilFinish();
        }

        @Override
        public State waitUntilFinish() {
            JStormUtils.sleepMs(localModeExecuteTimeSecs * 1000);
            try {
                return cancel();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public MetricResults metrics() {
            return null;
        }
    }

    private AbstractComponent getComponent(String id, TranslationContext.ExecutionGraphContext context) {
        AbstractComponent component = null;
        AdaptorBasicSpout spout = context.getSpout(id);
        if (spout != null) {
            component = spout;
        } else {
            AdaptorBasicBolt bolt = context.getBolt(id);
            if (bolt != null)
                component = bolt;
        }

        return component;
    }

    private StormTopology getTopology(StormPipelineOptions options, TranslationContext.ExecutionGraphContext context) {
        boolean isExactlyOnce = options.getExactlyOnceTopology();
        TopologyBuilder builder = isExactlyOnce ? new TransactionTopologyBuilder() : new TopologyBuilder();

        int parallelismNumber = options.getParallelismNumber();
        Map<String, AdaptorBasicSpout> spouts = context.getSpouts();
        for (String id : spouts.keySet()) {
            IRichSpout spout = getSpout(isExactlyOnce, spouts.get(id));
            builder.setSpout(id, spout, parallelismNumber);  
        }

        HashMap<String, BoltDeclarer> declarers = new HashMap<>();
        Iterable<Stream> streams = context.getStreams();
        LOG.info("streams=" + streams);
        for (Stream stream : streams) {
            String destBoltId = stream.getConsumer().getComponentId();
            IRichBolt bolt = getBolt(isExactlyOnce, context.getBolt(destBoltId));
            BoltDeclarer declarer = declarers.get(destBoltId);
            if (declarer == null) {
                declarer = builder.setBolt(destBoltId, bolt, parallelismNumber);
                declarers.put(destBoltId, declarer);
            }

            Stream.Grouping grouping = stream.getConsumer().getGrouping();
            String streamId = stream.getProducer().getStreamId();
            String srcBoltId = stream.getProducer().getComponentId();

            // add stream output declare for "from" component
            AbstractComponent component = getComponent(srcBoltId, context);
            if (grouping.getType().equals(Stream.Grouping.Type.FIELDS))
                component.addKVOutputField(streamId);
            else
                component.addOutputField(streamId);

            // "to" component declares grouping to "from" component 
            switch (grouping.getType()) {
            case SHUFFLE:
                declarer.shuffleGrouping(srcBoltId, streamId);
                break;
            case FIELDS:
                declarer.fieldsGrouping(srcBoltId, streamId, new Fields(grouping.getFields()));
                break;
            case ALL:
                declarer.allGrouping(srcBoltId, streamId);
                break;
            case DIRECT:
                declarer.directGrouping(srcBoltId, streamId);
                break;
            case GLOBAL:
                declarer.globalGrouping(srcBoltId, streamId);
                break;
            case LOCAL_OR_SHUFFLE:
                declarer.localOrShuffleGrouping(srcBoltId, streamId);
                break;
            case NONE:
                declarer.noneGrouping(srcBoltId, streamId);
                break;
            default:
                throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
            }

            // Subscribe grouping of water mark stream
            component.addOutputField(CommonInstance.BEAM_WATERMARK_STREAM_ID);
            declarer.allGrouping(srcBoltId, CommonInstance.BEAM_WATERMARK_STREAM_ID);
        }

        if (isExactlyOnce) {
            ((TransactionTopologyBuilder) builder).enableHdfs();
        }
        return builder.createTopology();
    }

    private IRichSpout getSpout(boolean isExactlyOnce, IRichSpout spout) {
        IRichSpout ret = null;
        if (isExactlyOnce) {
            if (spout instanceof UnboundedSourceSpout) {
                ret = new TxUnboundedSourceSpout((UnboundedSourceSpout) spout);
            } else {
                String error = String.format("The specified type(%s) is not supported in exactly once mode yet!", spout.getClass().toString());
                throw new RuntimeException(error);
            }
        } else {
            ret = spout;
        }
        return ret;
    }

    private IRichBolt getBolt(boolean isExactlyOnce, ExecutorsBolt bolt) {
        return isExactlyOnce ? new TxExecutorsBolt(bolt) : bolt;
    }
}