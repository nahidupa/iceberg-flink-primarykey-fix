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

  private static final TopicPartition CTL_TOPIC_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);

  @Test
  public void controlTopicOffsetsTrackTheHighestPositionConsumed() {
    TestChannelImpl channel = startChannel();

    consume(channel, 0, 1, 2, 3, 4);
    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 5L));

    // a re-read of the control topic, as happens when a rebalance resumes the consumer from the
    // last committed offsets, delivers records that were already handled. Only part of the replay
    // arrives in this batch, so the last record seen is behind the position reached above.
    consumer.seek(CTL_TOPIC_PARTITION, 1L);
    consume(channel, 1, 2);

    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 5L));
    assertThat(channel.received()).hasSize(5);
  }

  @Test
  public void committedControlTopicOffsetsDoNotRegressOnReplay() {
    TestChannelImpl channel = startChannel();

    consume(channel, 0, 1, 2, 3, 4);
    consumer.seek(CTL_TOPIC_PARTITION, 1L);
    consume(channel, 1, 2);

    channel.commitConsumerOffsets();

    // the offset committed for the group is what a restarted channel resumes from, and what the
    // coordinator stamps on the snapshot, so a regression here is durable
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
        .containsEntry(CTL_TOPIC_PARTITION, new OffsetAndMetadata(5L));
  }

  @Test
  public void controlTopicOffsetsAreTrackedPerPartition() {
    TestChannelImpl channel = startChannel();
    TopicPartition other = new TopicPartition(CTL_TOPIC_NAME, 1);
    consumer.rebalance(Lists.newArrayList(CTL_TOPIC_PARTITION, other));
    consumer.updateBeginningOffsets(ImmutableMap.of(CTL_TOPIC_PARTITION, 0L, other, 0L));

    addRecord(0, 0);
    addRecord(0, 1);
    addRecord(1, 0);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 2L, 1, 1L));
  }

  private TestChannelImpl startChannel() {
    TestChannelImpl channel =
        new TestChannelImpl(config, clientFactory, mock(SinkTaskContext.class));
    channel.start();

    // init consumer after subscribe()
    initConsumer();
    return channel;
  }

  @Test
  void ignoresReplayedOffsetsAndAcceptsNextOffset() {
    RecordingChannel channel = startRecordingChannel();
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
    RecordingChannel channel = startRecordingChannel();
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

  private RecordingChannel startRecordingChannel() {
    RecordingChannel channel = new RecordingChannel(config, clientFactory);
    channel.start();
    initConsumer();
    return channel;
  }

  private void consume(TestChannelImpl channel, int... offsets) {
    for (int offset : offsets) {
      addRecord(0, offset);
    }
    channel.consumeAvailable(Duration.ZERO);
  }

  private void addRecord(int partition, long offset) {
    addControlRecord(partition, offset, CONNECT_CONSUMER_GROUP_ID);
  }

  private void addControlRecord(int partition, long offset, String groupId) {
    Event event = new Event(groupId, new StartCommit(UUID.randomUUID()));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, partition, offset, "key", AvroUtil.encode(event)));
  }

  private static class TestChannelImpl extends Channel {
    private final List<Envelope> received = Lists.newArrayList();

    TestChannelImpl(
        IcebergSinkConfig config, KafkaClientFactory clientFactory, SinkTaskContext context) {
      super("test", CONNECT_CONSUMER_GROUP_ID + "-test", config, clientFactory, context);
    }

    @Override
    protected boolean receive(Envelope envelope) {
      received.add(envelope);
      return true;
    }

    List<Envelope> received() {
      return received;
    }
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
