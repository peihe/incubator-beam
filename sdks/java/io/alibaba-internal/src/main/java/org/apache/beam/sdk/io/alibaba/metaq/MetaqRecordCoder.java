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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * 
 * @author zhigang.lzg
 * @date 2017年4月26日
 * @param <T>
 */
public class MetaqRecordCoder<T> extends StructuredCoder<MetaqRecord<T>> {
  private static final long serialVersionUID = 1L;

  private ListCoder<T> listCoder;
  private MapCoder<String, String> mapCoder;

  public MetaqRecordCoder(Coder<T> coder) {
    listCoder = ListCoder.of(coder);
    mapCoder = MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
  }

  @Override
  public void encode(MetaqRecord<T> value, OutputStream outStream) throws CoderException, IOException {
    StringUtf8Coder.of().encode(value.getPartition(), outStream, Context.NESTED);
    StringUtf8Coder.of().encode(value.getOffset(), outStream, Context.NESTED);
    mapCoder.encode(value.getProperties(), outStream, Context.NESTED);
    listCoder.encode(value.getData(), outStream, Context.NESTED);
    VarIntCoder.of().encode(value.getBytesSize(), outStream, Context.NESTED);
    VarLongCoder.of().encode(value.getDataTimeStamp(), outStream, Context.NESTED);
  }

  @Override
  public MetaqRecord<T> decode(InputStream inStream) throws CoderException, IOException {
    return new MetaqRecord<T>(StringUtf8Coder.of().decode(inStream, Context.NESTED),
        StringUtf8Coder.of().decode(inStream, Context.NESTED), mapCoder.decode(inStream, Context.NESTED),
        listCoder.decode(inStream, Context.NESTED), VarIntCoder.of().decode(inStream, Context.NESTED),
        VarLongCoder.of().decode(inStream, Context.NESTED));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
  }

}
