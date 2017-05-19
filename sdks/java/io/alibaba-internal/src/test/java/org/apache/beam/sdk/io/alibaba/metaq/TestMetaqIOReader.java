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

import java.util.Date;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class TestMetaqIOReader {

  public static void main(String[] args) throws Exception {
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    //"galaxy_failover_topic_2"
    MetaqIO.Read<List<String>> read = MetaqIO.read("galaxy", "galaxy_test_beam_for_20170428");

    Date date = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000 * 720);
    read.setStartTime(date);
    read.setNameSerAdd("100.81.136.105:9876");

    PCollection<MetaqRecord<List<String>>> pCollection = pipeline.apply("metaq", read);

    pCollection.apply("outputResult", ParDo.of(new DoFn<MetaqRecord<List<String>>, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        System.out.println(c.element().toString());
      }
    }));

    pipeline.run();

  }
}
