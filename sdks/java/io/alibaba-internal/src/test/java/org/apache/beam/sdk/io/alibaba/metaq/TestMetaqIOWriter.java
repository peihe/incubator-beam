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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class TestMetaqIOWriter {

  public static void main(String[] args) throws Exception {
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    String[] strs = new String[]{
        "16:02:05insert2017-03-17",
        "16:02:05.9653392017-03-17",
        "16:02:05.965339208820285007554220882028357179401031我是szpipl0112017-03-17",
        "16:02:05.96533910",
        "14897377260620492761add_friend_reqadd_friend_req2017-03-17",
        "16:02:05insert2017-03-17",
        "16:02:05.9714062017-03-17",
        "16:02:05.9714062088202835717940208820285007554210320112017-03-17",
        "16:02:05.97140610"
    };

    MetaqIO.Write<String> write = MetaqIO.write("galaxy",
        "galaxy_test_beam_for_20170428", StringUtf8Coder.of());
    write.setNameSerAdd("100.81.136.105:9876");
    PCollection<String> pCollection = pipeline.apply(Create.of("16:02:05insert2017-03-17", strs));
    pCollection.apply(write);

    pipeline.run();


  }
}
