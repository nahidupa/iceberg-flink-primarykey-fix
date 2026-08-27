/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class TestChannel extends ChannelTestBase {

  @Test
  public void testControlTopicOffsetTrackingIsMonotonicAcrossReRead() {
    when(config.commitIntervalMs()).thenReturn(0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    TestChannelImpl channel = new TestChannelImpl(config, clientFactory, context);
    channel.start();
    initConsumer();

    TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);

    // consume records with increasing offsets
    addRecord(1);
    addRecord(2);
    addRecord(3);
    channel.consumeAvailable(Duration.ofMillis(1));

    // position should track the next offset to consume
    assertThat(channel.controlTopicOffsets()).containsEntry(0, 4L);

    // simulate a task restart: the consumer seeks back to the last committed group offset and
    // re-reads the control topic. Only part of the re-read has arrived when we observe the map,
    // which is the interleaving that previously let the tracked position regress.
    consumer.seek(tp, 1);
    addRecord(1);
    addRecord(2);
    channel.consumeAvailable(Duration.ofMillis(1));

    // the tracked position must not regress below the high watermark
    assertThat(channel.controlTopicOffsets()).containsEntry(0, 4L);
  }

  private void addRecord(long offset) {
    Event event = new Event(config.connectGroupId(), new StartCommit(UUID.randomUUID()));
    byte[] bytes = AvroUtil.encode(event);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset, "key", bytes));
  }

  /** Test subclass that exposes the control topic offset map for assertions. */
  private static class TestChannelImpl extends Channel {
    TestChannelImpl(
        IcebergSinkConfig config, KafkaClientFactory clientFactory, SinkTaskContext context) {
      super("test", "test-group", config, clientFactory, context);
    }

    @Override
    protected boolean receive(Envelope envelope) {
      return false;
    }
  }
}
