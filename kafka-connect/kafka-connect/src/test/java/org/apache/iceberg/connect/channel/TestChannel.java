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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

class TestChannel extends ChannelTestBase {

  @Test
  void ignoresReplayedOffsetsAndAcceptsNextOffset() {
    RecordingChannel channel = startChannel();
    TopicPartition partition = new TopicPartition(CTL_TOPIC_NAME, 0);
    addControlRecord(0, 1L, CONNECT_CONSUMER_GROUP_ID);
    addControlRecord(0, 2L, CONNECT_CONSUMER_GROUP_ID);
    channel.consumeAvailable(Duration.ZERO);

    consumer.seek(partition, 1L);
    addControlRecord(0, 1L, CONNECT_CONSUMER_GROUP_ID);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received).extracting(Envelope::offset).containsExactly(1L, 2L);
    assertThat(channel.controlTopicOffsets()).containsExactlyEntriesOf(ImmutableMap.of(0, 3L));
    channel.commitConsumerOffsets();
    assertThat(consumer.committed(ImmutableSet.of(partition)))
        .containsEntry(partition, new OffsetAndMetadata(3L));

    addControlRecord(0, 2L, CONNECT_CONSUMER_GROUP_ID);
    addControlRecord(0, 3L, CONNECT_CONSUMER_GROUP_ID);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received).extracting(Envelope::offset).containsExactly(1L, 2L, 3L);
    assertThat(channel.controlTopicOffsets()).containsExactlyEntriesOf(ImmutableMap.of(0, 4L));
  }

  @Test
  void tracksPartitionsIndependentlyAndFiltersOtherGroups() {
    RecordingChannel channel = startChannel();
    TopicPartition firstPartition = new TopicPartition(CTL_TOPIC_NAME, 0);
    TopicPartition secondPartition = new TopicPartition(CTL_TOPIC_NAME, 1);
    consumer.rebalance(ImmutableList.of(firstPartition, secondPartition));
    consumer.updateBeginningOffsets(ImmutableMap.of(secondPartition, 0L));

    addControlRecord(0, 10L, "other-group");
    addControlRecord(1, 0L, CONNECT_CONSUMER_GROUP_ID);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received)
        .extracting(Envelope::partition, Envelope::offset)
        .containsExactly(tuple(1, 0L));
    assertThat(channel.controlTopicOffsets())
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(0, 11L, 1, 1L));

    consumer.seek(firstPartition, 10L);
    addControlRecord(0, 10L, "other-group");
    addControlRecord(0, 11L, CONNECT_CONSUMER_GROUP_ID);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received)
        .extracting(Envelope::partition, Envelope::offset)
        .containsExactly(tuple(1, 0L), tuple(0, 11L));
    assertThat(channel.controlTopicOffsets())
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(0, 12L, 1, 1L));
  }

  private RecordingChannel startChannel() {
    RecordingChannel channel = new RecordingChannel(config, clientFactory);
    channel.start();
    initConsumer();
    return channel;
  }

  private void addControlRecord(int partition, long offset, String groupId) {
    Event event = new Event(groupId, new StartCommit(UUID.randomUUID()));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, partition, offset, "key", AvroUtil.encode(event)));
  }

  private static class RecordingChannel extends Channel {
    private final List<Envelope> received = Lists.newArrayList();

    private RecordingChannel(IcebergSinkConfig config, KafkaClientFactory clientFactory) {
      super("test", CONNECT_CONSUMER_GROUP_ID, config, clientFactory, mock(SinkTaskContext.class));
    }

    @Override
    protected boolean receive(Envelope envelope) {
      received.add(envelope);
      return true;
    }
  }
}
