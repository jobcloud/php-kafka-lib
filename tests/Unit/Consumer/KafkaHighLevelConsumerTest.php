<?php

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Consumer;

use Jobcloud\Kafka\Consumer\KafkaHighLevelConsumer;
use Jobcloud\Kafka\Consumer\TopicSubscriptionInterface;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Kafka\Consumer\TopicSubscription;
use Jobcloud\Kafka\Exception\KafkaConsumerAssignmentException;
use Jobcloud\Kafka\Exception\KafkaConsumerRequestException;
use Jobcloud\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message;
use RdKafka\Metadata as RdKafkaMetadata;
use RdKafka\Metadata\Collection as RdKafkaMetadataCollection;
use RdKafka\Metadata\Partition as RdKafkaMetadataPartition;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

/**
 * @covers \Jobcloud\Kafka\Consumer\AbstractKafkaConsumer
 * @covers \Jobcloud\Kafka\Consumer\KafkaHighLevelConsumer
 */
final class KafkaHighLevelConsumerTest extends TestCase
{

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccess(): void
    {
        $topics = [new TopicSubscription('testTopic'), new TopicSubscription('testTopic2')];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturnOnConsecutiveCalls($topics, []);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('subscribe')->with(['testTopic', 'testTopic2']);

        $kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccessWithParam(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::never())->method('getTopicSubscriptions');
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('subscribe')->with(['testTopic3']);

        $kafkaConsumer->subscribe([new TopicSubscription('testTopic3')]);
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccessWithAssignmentWithPartitions(): void
    {
        $topics = [new TopicSubscription('testTopic', [1,2], RD_KAFKA_OFFSET_BEGINNING)];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturnOnConsecutiveCalls([], $topics);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('assign');

        $kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccessWithAssignmentWithOffsetOnly(): void
    {
        $partitions = [
            $this->getMetadataPartitionMock(1),
            $this->getMetadataPartitionMock(2)
        ];

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);

        /** @var RdKafkaMetadataTopic|MockObject $rdKafkaMetadataTopicMock */
        $rdKafkaMetadataTopicMock = $this->createMock(RdKafkaMetadataTopic::class);
        $rdKafkaMetadataTopicMock
            ->expects(self::once())
            ->method('getPartitions')
            ->willReturn($partitions);

        /** @var RdKafkaMetadata|MockObject $rdKafkaMetadataMock */
        $rdKafkaMetadataMock = $this->createMock(RdKafkaMetadata::class);
        $rdKafkaMetadataMock
            ->expects(self::once())
            ->method('getTopics')
            ->willReturnCallback(
                function () use ($rdKafkaMetadataTopicMock) {
                    /** @var RdKafkaMetadataCollection|MockObject $collection */
                    $collection = $this->createMock(RdKafkaMetadataCollection::class);
                    $collection
                        ->expects(self::once())
                        ->method('current')
                        ->willReturn($rdKafkaMetadataTopicMock);

                    return $collection;
                }
            );

        $topics = [new TopicSubscription('testTopic', [], RD_KAFKA_OFFSET_END)];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturnOnConsecutiveCalls([], $topics);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('assign')->with(
            $this->callback(
                function (array $assignment) {
                    self::assertCount(2, $assignment);
                    return true;
                }
            )
        );
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $rdKafkaConsumerTopicMock, 10000)
            ->willReturn($rdKafkaMetadataMock);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('testTopic')
            ->willReturn($rdKafkaConsumerTopicMock);


        $kafkaConsumer->subscribe();
    }


    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeFailureOnMixedSubscribe(): void
    {
        $topics = [
            new TopicSubscription('testTopic'),
            new TopicSubscription('anotherTestTopic', [1,2], RD_KAFKA_OFFSET_BEGINNING)
        ];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturn($topics);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::never())->method('subscribe');
        $rdKafkaConsumerMock->expects(self::never())->method('assign');


        $this->expectException(KafkaConsumerSubscriptionException::class);
        $this->expectExceptionMessage(KafkaConsumerSubscriptionException::MIXED_SUBSCRIPTION_EXCEPTION_MESSAGE);

        $kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeFailure(): void
    {
        $topics = [new TopicSubscription('testTopic')];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturn($topics);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with(['testTopic'])
            ->willThrowException(new RdKafkaException('Error', 100));

        $this->expectException(KafkaConsumerSubscriptionException::class);
        $this->expectExceptionCode(100);
        $this->expectExceptionMessage('Error');

        $kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testUnsubscribeSuccesss(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('unsubscribe');

        $kafkaConsumer->unsubscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testUnsubscribeSuccesssConsumeFails(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage(KafkaConsumerConsumeException::NOT_SUBSCRIBED_EXCEPTION_MESSAGE);

        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('unsubscribe');

        $kafkaConsumer->unsubscribe();

        $kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testUnsubscribeFailure(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('unsubscribe')
            ->willThrowException(new RdKafkaException('Error', 100));

        $this->expectException(KafkaConsumerSubscriptionException::class);
        $this->expectExceptionCode(100);
        $this->expectExceptionMessage('Error');


        $kafkaConsumer->unsubscribe();
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitSuccesss(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(1))->method('getOffset')->willReturn(0);
        $message->expects(self::exactly(1))->method('getTopicName')->willReturn('test');
        $message->expects(self::exactly(1))->method('getPartition')->willReturn(1);
        $message2 = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message2->expects(self::exactly(1))->method('getOffset')->willReturn(1);
        $message2->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $message2->expects(self::exactly(2))->method('getPartition')->willReturn(1);
        $message3 = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message3->expects(self::exactly(2))->method('getOffset')->willReturn(2);
        $message3->expects(self::exactly(1))->method('getTopicName')->willReturn('test');
        $message3->expects(self::exactly(1))->method('getPartition')->willReturn(1);
        $message4 = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message4->expects(self::exactly(1))->method('getOffset')->willReturn(0);
        $message4->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $message4->expects(self::exactly(2))->method('getPartition')->willReturn(2);


        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $rdKafkaConsumerMock->expects(self::once())->method('commit')->with(
            $this->callback(
                function (array $topicPartitions) {
                    self::assertCount(2, $topicPartitions);
                    self::assertInstanceOf(RdKafkaTopicPartition::class, $topicPartitions['test-1']);
                    self::assertInstanceOf(RdKafkaTopicPartition::class, $topicPartitions['test-2']);
                    self::assertEquals(3, $topicPartitions['test-1']->getOffset());
                    self::assertEquals(1, $topicPartitions['test-2']->getOffset());

                    return true;
                }
            )
        );

        $kafkaConsumer->commit([$message2, $message, $message3, $message4]);
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitSingleSuccesss(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(1))->method('getOffset')->willReturn(0);
        $message->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $message->expects(self::exactly(2))->method('getPartition')->willReturn(1);


        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $rdKafkaConsumerMock->expects(self::once())->method('commit')->with(
            $this->callback(
                function (array $topicPartitions) {
                    self::assertCount(1, $topicPartitions);
                    self::assertInstanceOf(RdKafkaTopicPartition::class, $topicPartitions['test-1']);
                    self::assertEquals(1, $topicPartitions['test-1']->getOffset());
                    return true;
                }
            )
        );

        $kafkaConsumer->commit($message);
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitAsyncSuccesss(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $message = $this->createMock(KafkaConsumerMessageInterface::class);

        $rdKafkaConsumerMock->expects(self::once())->method('commitAsync');

        $kafkaConsumer->commitAsync([$message]);
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitFails(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $message = $this->createMock(KafkaConsumerMessageInterface::class);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('commit')
            ->willThrowException(new RdKafkaException('Failure', 99));

        $this->expectException(KafkaConsumerCommitException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Failure');

        $kafkaConsumer->commit([$message]);
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testAssignSuccess(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $topicPartitions = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('assign')
            ->with($topicPartitions);

        $kafkaConsumer->assign($topicPartitions);
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testAssignFail(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $topicPartitions = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('assign')
            ->with($topicPartitions)
            ->willThrowException(new RdKafkaException('Failure', 99));

        $this->expectException(KafkaConsumerAssignmentException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Failure');

        $kafkaConsumer->assign($topicPartitions);
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testGetAssignment(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $topicPartitions = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getAssignment')
            ->willReturn($topicPartitions);

        $this->assertEquals($topicPartitions, $kafkaConsumer->getAssignment());
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testGetAssignmentException(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getAssignment')
            ->willThrowException(new RdKafkaException('Fail', 99));

        $this->expectException(KafkaConsumerAssignmentException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Fail');
        $kafkaConsumer->getAssignment();
    }

    public function testKafkaConsumeWithDecode(): void
    {
        $message = new Message();
        $message->key = 'test';
        $message->payload = null;
        $message->topic_name = 'test_topic';
        $message->partition = '9';
        $message->offset = '501';
        $message->timestamp = '500';
        $message->headers = 'header';
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;

        $topics = [new TopicSubscription('testTopic')];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with(['testTopic']);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn($message);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturnOnConsecutiveCalls($topics, []);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $decoderMock->expects(self::once())->method('decode')->with(
            $this->callback(
                function (KafkaConsumerMessageInterface $message) {
                    self::assertEquals('test', $message->getKey());
                    self::assertNull($message->getBody());
                    self::assertEquals('test_topic', $message->getTopicName());
                    self::assertEquals(9, $message->getPartition());
                    self::assertEquals(501, $message->getOffset());
                    self::assertEquals(500, $message->getTimestamp());
                    self::assertEquals(['header'], $message->getHeaders());

                    return true;
                }
            )
        );
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $kafkaConsumer->subscribe();
        $kafkaConsumer->consume();
    }

    public function testKafkaConsumeWithoutDecode(): void
    {
        $message = new Message();
        $message->key = 'test';
        $message->payload = null;
        $message->topic_name = 'test_topic';
        $message->partition = 9;
        $message->offset = 501;
        $message->timestamp = 500;
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;

        $topics = [new TopicSubscription('testTopic')];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with(['testTopic']);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn($message);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::exactly(2))->method('getTopicSubscriptions')->willReturnOnConsecutiveCalls($topics, []);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $decoderMock->expects(self::never())->method('decode');
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $kafkaConsumer->subscribe();
        $kafkaConsumer->consume(10000, false);
    }

    public function testDecodeMessage(): void
    {
        $messageMock = $this->createMock(KafkaConsumerMessageInterface::class);
        $messageMock->expects(self::once())->method('getKey')->willReturn('test');
        $messageMock->expects(self::once())->method('getBody')->willReturn('some body');
        $messageMock->expects(self::once())->method('getTopicName')->willReturn('test_topic');
        $messageMock->expects(self::once())->method('getPartition')->willReturn(9);
        $messageMock->expects(self::once())->method('getOffset')->willReturn(501);
        $messageMock->expects(self::once())->method('getTimestamp')->willReturn(500);
        $messageMock->expects(self::once())->method('getHeaders')->willReturn(['some' => 'header']);

        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $decoderMock->expects(self::once())->method('decode')->with(
            $this->callback(
                function (KafkaConsumerMessageInterface $message) {
                    self::assertEquals('test', $message->getKey());
                    self::assertEquals('some body', $message->getBody());
                    self::assertEquals('test_topic', $message->getTopicName());
                    self::assertEquals(9, $message->getPartition());
                    self::assertEquals(501, $message->getOffset());
                    self::assertEquals(500, $message->getTimestamp());
                    self::assertEquals(['some' => 'header'], $message->getHeaders());
                    return true;
                }
            )
        );
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $kafkaConsumer->decodeMessage($messageMock);
    }

    /**
     * @throws KafkaConsumerRequestException
     */
    public function testGetCommittedOffsets(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $committedOffsets = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getCommittedOffsets')
            ->with($committedOffsets, 1)
            ->willReturn($committedOffsets);

        $this->assertEquals($committedOffsets, $kafkaConsumer->getCommittedOffsets($committedOffsets, 1));
    }

    /**
     * @throws KafkaConsumerRequestException
     */
    public function testGetCommittedOffsetsException(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getCommittedOffsets')
            ->willThrowException(new RdKafkaException('Fail', 99));

        $this->expectException(KafkaConsumerRequestException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Fail');
        $kafkaConsumer->getCommittedOffsets([], 1);
    }

    /**
     * @return void
     */
    public function testGetOffsetPositions(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getOffsetPositions')
            ->with([])
            ->willReturn([]);

        $kafkaConsumer->getOffsetPositions([]);
    }

    /**
     * @return void
     */
    public function testClose(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $rdKafkaConsumerMock->expects(self::once())->method('close');

        $kafkaConsumer->close();
    }

    /**
     * @return void
     */
    public function testGetTopicSubscriptionsReturnsTopicSubscriptions(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);

        $topicSubscriptionsMock = [
            $this->createMock(TopicSubscriptionInterface::class),
            $this->createMock(TopicSubscriptionInterface::class)
        ];

        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn($topicSubscriptionsMock);

        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        self::assertSame($topicSubscriptionsMock, $kafkaConsumer->getTopicSubscriptions());
    }

    /**
     * @param int $partitionId
     * @return RdKafkaMetadataPartition|MockObject
     */
    private function getMetadataPartitionMock(int $partitionId): RdKafkaMetadataPartition
    {
        $partitionMock = $this->getMockBuilder(RdKafkaMetadataPartition::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['getId'])
            ->getMock();

        $partitionMock
            ->expects(self::once())
            ->method('getId')
            ->willReturn($partitionId);

        return $partitionMock;
    }
}
