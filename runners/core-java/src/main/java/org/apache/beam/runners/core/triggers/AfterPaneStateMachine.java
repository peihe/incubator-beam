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
package org.apache.beam.runners.core.triggers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Objects;
import org.apache.beam.runners.core.MergingStateAccessor;
import org.apache.beam.runners.core.StateAccessor;
import org.apache.beam.runners.core.StateMerging;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.triggers.TriggerStateMachine.OnceTriggerStateMachine;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link TriggerStateMachine}s that fire based on properties of the elements in the current pane.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterPaneStateMachine extends OnceTriggerStateMachine {

  private static final StateTag<CombiningState<KV<Long, Instant>, long[], KV<Long, Long>>>
      ELEMENTS_IN_PANE_TAG = StateTags.makeSystemTagInternal(StateTags.combiningValueFromInputInternal(
          "countWithTimestamp",
          KvCoder.of(BigEndianLongCoder.of(), InstantCoder.of()),
          new CombineLongFnWithTimestamp()));

  private final int countElems;

  private final long maxElemIntervalMillis;

  private BoundedWindow lastWindow;

  private boolean shouldFire;

  private AfterPaneStateMachine(int countElems, long maxElemIntervalMillis) {
    super(null);
    this.countElems = countElems;
    this.maxElemIntervalMillis = maxElemIntervalMillis;
    this.shouldFire = false;
  }

  /**
   * The number of elements after which this trigger may fire.
   */
  public int getElementCount() {
    return countElems;
  }

  /**
   * Creates a trigger that fires when the pane contains at least {@code countElems} elements.
   */
  public static AfterPaneStateMachine elementCountAtLeast(int countElems) {
    return new AfterPaneStateMachine(countElems, BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
  }

  public AfterPaneStateMachine elementIntervalAtMost(Duration maxElemInterval) {
    return new AfterPaneStateMachine(countElems, maxElemInterval.getMillis());
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    CombiningState<KV<Long, Instant>, long[], KV<Long, Long>> state =
        c.state().access(ELEMENTS_IN_PANE_TAG).readLater();
    state.add(KV.of(1L, c.eventTimestamp()));
    long[] accum = state.getAccum();
    shouldFire = accum[0] >= countElems || (accum[1] - accum[2]) >= maxElemIntervalMillis;
    lastWindow = c.window();
  }

  @Override
  public void prefetchOnMerge(MergingStateAccessor<?, ?> state) {
    super.prefetchOnMerge(state);
    StateMerging.prefetchCombiningValues(state, ELEMENTS_IN_PANE_TAG);
  }

  @Override
  public void onMerge(OnMergeContext context) throws Exception {
    // If we've already received enough elements and finished in some window,
    // then this trigger is just finished.
    if (context.trigger().finishedInAnyMergingWindow()) {
      context.trigger().setFinished(true);
      StateMerging.clear(context.state(), ELEMENTS_IN_PANE_TAG);
      return;
    }

    // Otherwise, compute the sum of elements in all the active panes.
    StateMerging.mergeCombiningValues(context.state(), ELEMENTS_IN_PANE_TAG);
  }

  @Override
  public void prefetchShouldFire(StateAccessor<?> state) {
    state.access(ELEMENTS_IN_PANE_TAG).readLater();
  }

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    // WARN: Assume that state key will not change after invoking onElement till shouldFire
    if (lastWindow == null || !lastWindow.equals(context.window())) {
      KV<Long, Long> countAndInterval = context.state().access(ELEMENTS_IN_PANE_TAG).read();
      shouldFire = countAndInterval.getKey() >= countElems
          || countAndInterval.getValue() >= maxElemIntervalMillis;
      lastWindow = context.window();
    }
    return shouldFire;
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.state().access(ELEMENTS_IN_PANE_TAG).clear();
    shouldFire = countElems == 0;
  }

  @Override
  public boolean isCompatible(TriggerStateMachine other) {
    return this.equals(other);
  }

  @Override
  public String toString() {
    return "AfterPane.elementCountAtLeast(" + countElems + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterPaneStateMachine)) {
      return false;
    }
    AfterPaneStateMachine that = (AfterPaneStateMachine) obj;
    return this.countElems == that.countElems
        && this.maxElemIntervalMillis == that.maxElemIntervalMillis;
  }

  @Override
  public int hashCode() {
    return Objects.hash(countElems, maxElemIntervalMillis);
  }

  @Override
  protected void onOnlyFiring(TriggerStateMachine.TriggerContext context) throws Exception {
    clear(context);
  }

  /**
   * {@link TriggerStateMachine}s that fire based on properties of the elements in the current pane.
   */
  public static class CombineLongFnWithTimestamp
      extends Combine.CombineFn<KV<Long, Instant>, long[], KV<Long, Long>> {

    private static final Coder<long[]> ACCUM_CODER = getCoder();

    private static final Coder<KV<Long, Long>> OUTPUT_CODER =
        KvCoder.of(BigEndianLongCoder.of(), BigEndianLongCoder.of());

    @Override
    public long[] createAccumulator() {
      return new long[] {0L,
          BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis(),
          BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()};
    }

    @Override
    public long[] addInput(long[] accumulator, KV<Long, Instant> input) {
      accumulator[0] += input.getKey();
      accumulator[2] = accumulator[1];
      accumulator[1] = input.getValue().getMillis();
      return accumulator;
    }

    @Override
    public long[] mergeAccumulators(Iterable<long[]> accumulators) {
      Iterator<long[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        long[] running = iter.next();
        while (iter.hasNext()) {
          running[0] += iter.next()[0];
          long timestamp = iter.next()[1];
          if (timestamp > running[1]) {
            running[2] = running[1];
            running[1] = timestamp;
          } else if (timestamp > running[2]) {
            running[2] = timestamp;
          }
        }
        return running;
      }
    }

    @Override
    public KV<Long, Long> extractOutput(long[] accumulator) {
      return KV.of(accumulator[0], accumulator[1] - accumulator[2]);
    }

    @Override
    public Coder<long[]> getAccumulatorCoder(CoderRegistry registry,
                                             Coder<KV<Long, Instant>> inputCoder) {
      return ACCUM_CODER;
    }

    @Override
    public Coder<KV<Long, Long>> getDefaultOutputCoder(CoderRegistry registry,
                                                       Coder<KV<Long, Instant>> inputCoder) {
      return OUTPUT_CODER;
    }

    public static Coder<long[]> getCoder() {
      return new CustomCoder<long[]>() {
        @Override
        public void encode(long[] accumulator, OutputStream outStream) throws CoderException, IOException {
          BigEndianLongCoder.of().encode(accumulator[0], outStream);
          BigEndianLongCoder.of().encode(accumulator[1], outStream);
          BigEndianLongCoder.of().encode(accumulator[2], outStream);
        }

        @Override
        public long[] decode(InputStream inStream) throws CoderException, IOException {
          return new long[] {
              BigEndianLongCoder.of().decode(inStream),
              BigEndianLongCoder.of().decode(inStream),
              BigEndianLongCoder.of().decode(inStream)
          };
        }
      };
    }

  }
}
