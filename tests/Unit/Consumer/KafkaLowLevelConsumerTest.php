<?php

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Consumer;

use Jobcloud\Kafka\Consumer\KafkaLowLevelConsumer;
use Jobcloud\Kafka\Consumer\TopicSubscriptionInterface;
use Jobcloud\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Kafka\Consumer\TopicSubscription;
use Jobcloud\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Kafka\Message\KafkaMessageInterface;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\Metadata as RdKafkaMetadata;
use RdKafka\Metadata\Collection as RdKafkaMetadataCollection;
use RdKafka\Metadata\Partition as RdKafkaMetadataPartition;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Queue as RdKafkaQueue;

/**
 * @covers \Jobcloud\Kafka\Consumer\AbstractKafkaConsumer
 * @covers \Jobcloud\Kafka\Consumer\KafkaLowLevelConsumer
 */
final class KafkaLowLevelConsumerTest extends TestCase
{

    /** @var RdKafkaQueue|MockObject */
    private $rdKafkaQueueMock;

    /** @var RdKafkaLowLevelConsumer|MockObject */
    private $rdKafkaConsumerMock;

    /** @var KafkaConfiguration|MockObject */
    private $kafkaConfigurationMock;

    /** @var DecoderInterface|MockObject $decoderMock */
    private $decoderMock;

    /** @var KafkaLowLevelConsumer */
    private $kafkaConsumer;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->rdKafkaQueueMock = $this->createMock(RdKafkaQueue::class);
        $this->rdKafkaConsumerMock = $this->createMock(RdKafkaLowLevelConsumer::class);
        $this->rdKafkaConsumerMock
            ->expects(self::atLeastOnce())
            ->method('newQueue')
            ->willReturn($this->rdKafkaQueueMock);
        $this->kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $this->kafkaConfigurationMock->expects(self::any())->method('dump')->willReturn([]);
        $this->decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock, $this->decoderMock);
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerSubscriptionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeWithTopicSubscriptionWithNoPartitionsIsSuccessful(): void
    {
        $partitions = [
            $this->getMetadataPartitionMock(1),
            $this->getMetadataPartitionMock(2)
        ];

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $rdKafkaMessageMock->topic_name = 'sample_topic';
        $rdKafkaMessageMock->partition = 0;
        $rdKafkaMessageMock->offset = 1;
        $rdKafkaMessageMock->timestamp = 1;
        $rdKafkaMessageMock->headers = null;
        $rdKafkaMessageMock
            ->expects(self::never())
            ->method('errstr');

        $kafkaMessageMock = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $kafkaMessageMock->expects(self::once())->method('getBody')->willReturn($rdKafkaMessageMock->payload);
        $kafkaMessageMock->expects(self::once())->method('getOffset')->willReturn($rdKafkaMessageMock->offset);
        $kafkaMessageMock->expects(self::once())->method('getPartition')->willReturn($rdKafkaMessageMock->partition);

        $this->decoderMock->expects(self::once())->method('decode')->willReturn($kafkaMessageMock);

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

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([new TopicSubscription('test-topic')]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $rdKafkaConsumerTopicMock, 10000)
            ->willReturn($rdKafkaMetadataMock);
        $this->rdKafkaConsumerMock
            ->expects(self::exactly(2))
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $message = $this->kafkaConsumer->consume();

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $message);
        self::assertInstanceOf(KafkaMessageInterface::class, $message);

        self::assertEquals($rdKafkaMessageMock->payload, $message->getBody());
        self::assertEquals($rdKafkaMessageMock->offset, $message->getOffset());
        self::assertEquals($rdKafkaMessageMock->partition, $message->getPartition());
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerSubscriptionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeThrowsEofExceptionIfQueueConsumeReturnsNull(): void
    {
        self::expectException(KafkaConsumerEndOfPartitionException::class);
        self::expectExceptionCode(RD_KAFKA_RESP_ERR__PARTITION_EOF);
        self::expectExceptionMessage(rd_kafka_err2str(RD_KAFKA_RESP_ERR__PARTITION_EOF));

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn(null);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerSubscriptionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeDedicatedEofException(): void
    {
        self::expectException(KafkaConsumerEndOfPartitionException::class);
        self::expectExceptionCode(RD_KAFKA_RESP_ERR__PARTITION_EOF);
        self::expectExceptionMessage(rd_kafka_err2str(RD_KAFKA_RESP_ERR__PARTITION_EOF));

        $message = new RdKafkaMessage();
        $message->err = RD_KAFKA_RESP_ERR__PARTITION_EOF;

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn($message);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerSubscriptionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeDedicatedTimeoutException(): void
    {
        self::expectException(KafkaConsumerTimeoutException::class);
        self::expectExceptionCode(RD_KAFKA_RESP_ERR__TIMED_OUT);
        self::expectExceptionMessage(rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT));

        $message = new RdKafkaMessage();
        $message->err = RD_KAFKA_RESP_ERR__TIMED_OUT;

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(1000)
            ->willReturn($message);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume(1000);
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerSubscriptionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeThrowsExceptionIfConsumedMessageHasNoTopicAndErrorCodeIsNotOkay(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('Unknown error');

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN;
        $rdKafkaMessageMock->partition = 1;
        $rdKafkaMessageMock->offset = 103;
        $rdKafkaMessageMock->topic_name = null;
        $rdKafkaMessageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn('Unknown error');

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with(1, 103, $this->rdKafkaQueueMock)
            ->willReturn(null);

        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerSubscriptionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeFailThrowsException(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('Unknown error');

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = -1;
        $rdKafkaMessageMock->partition = 1;
        $rdKafkaMessageMock->offset = 103;
        $rdKafkaMessageMock->topic_name = 'test-topic';
        $rdKafkaMessageMock->timestamp = 1;
        $rdKafkaMessageMock->headers = ['key' => 'value'];
        $rdKafkaMessageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn('Unknown error');

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with(1, 103, $this->rdKafkaQueueMock)
            ->willReturn(null);

        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(10000)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerTimeoutException
     * @return void
     */
    public function testConsumeThrowsExceptionIfConsumerIsCurrentlyNotSubscribed(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('This consumer is currently not subscribed');

        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @throws \ReflectionException
     * @return void
     */
    public function testSubscribeEarlyReturnsIfAlreadySubscribed(): void
    {
        $subscribedProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'subscribed');
        $subscribedProperty->setAccessible(true);
        $subscribedProperty->setValue($this->kafkaConsumer, true);

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testSubscribeConvertsExtensionExceptionToLibraryException(): void
    {
        self::expectException(KafkaConsumerSubscriptionException::class);
        self::expectExceptionMessage('TEST_EXCEPTION_MESSAGE');

        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willThrowException(new RdKafkaException('TEST_EXCEPTION_MESSAGE'));

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testSubscribeUseExistingTopicsForResubscribe(): void
    {
        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::exactly(2))
            ->method('consumeQueueStart')
            ->with(1, 103, $this->rdKafkaQueueMock)
            ->willReturn(null);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeStop')
            ->with(1)
            ->willReturn(null);

        $this->kafkaConfigurationMock
            ->expects(self::exactly(3))
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();

        self::assertTrue($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->unsubscribe();

        self::assertFalse($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerCommitException
     * @throws \ReflectionException
     * @return void
     */
    public function testCommitWithMessageStoresOffsetOfIt(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getTopicName')->willReturn('test-topic');

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('offsetStore')
            ->with($message->getPartition(), $message->getOffset());

        $rdKafkaConsumerMockProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'topics');
        $rdKafkaConsumerMockProperty->setAccessible(true);
        $rdKafkaConsumerMockProperty->setValue(
            $this->kafkaConsumer,
            ['test-topic' => $rdKafkaConsumerTopicMock]
        );

        $this->kafkaConsumer->commit($message);
    }

    /**
     * @throws KafkaConsumerCommitException
     * @throws \ReflectionException
     * @return void
     */
    public function testCommitWithInvalidObjectThrowsExceptionAndDoesNotTriggerCommit(): void
    {
        self::expectException(KafkaConsumerCommitException::class);
        self::expectExceptionMessage(
            'Provided message (index: 0) is not an instance of "Jobcloud\Kafka\Message\KafkaConsumerMessage"'
        );

        $message = new \stdClass();

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::never())
            ->method('offsetStore');

        $rdKafkaConsumerMockProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'topics');
        $rdKafkaConsumerMockProperty->setAccessible(true);
        $rdKafkaConsumerMockProperty->setValue($this->kafkaConsumer, ['test-topic' => $rdKafkaConsumerTopicMock]);

        $this->kafkaConsumer->commit($message);
    }

    /**
     * @return void
     */
    public function testUnsubscribeEarlyReturnsIfAlreadyUnsubscribed(): void
    {
        self::assertFalse($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->unsubscribe();
    }

    /**
     * @return void
     */
    public function testIsSubscribedReturnsDefaultSubscriptionState(): void
    {
        self::assertFalse($this->kafkaConsumer->isSubscribed());
    }

    /**
     * @return void
     */
    public function testGetConfiguration(): void
    {
        self::assertIsArray($this->kafkaConsumer->getConfiguration());
    }

    /**
     * @return void
     */
    public function testGetFirstOffsetForTopicPartition(): void
    {
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('queryWatermarkOffsets')
            ->with('test-topic', 1, 0, 0, 1000)
            ->willReturnCallback(
                function (string $topic, int $partition, int &$lowOffset, int &$highOffset, int $timeoutMs) {
                    $lowOffset++;
                }
            );

        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock, $this->decoderMock);

        $lowOffset = $this->kafkaConsumer->getFirstOffsetForTopicPartition('test-topic', 1, 1000);

        $this->assertEquals(1, $lowOffset);
    }

    /**
     * @return void
     */
    public function testGetLastOffsetForTopicPartition(): void
    {
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('queryWatermarkOffsets')
            ->with('test-topic', 1, 0, 0, 1000)
            ->willReturnCallback(
                function (string $topic, int $partition, int &$lowOffset, int &$highOffset, int $timeoutMs) {
                    $highOffset += 5;
                }
            );

        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock, $this->decoderMock);

        $lowOffset = $this->kafkaConsumer->getLastOffsetForTopicPartition('test-topic', 1, 1000);

        $this->assertEquals(5, $lowOffset);
    }

    /**
     * @return void
     */
    public function testGetTopicSubscriptionsReturnsTopicSubscriptions(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaLowLevelConsumer::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);

        $topicSubscriptionsMock = [
            $this->createMock(TopicSubscriptionInterface::class),
            $this->createMock(TopicSubscriptionInterface::class)
        ];

        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn($topicSubscriptionsMock);

        $kafkaConsumer = new KafkaLowLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

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

    /**
     * @return void
     */
    public function testOffsetsForTimes(): void
    {
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('offsetsForTimes')
            ->with([], 1000)
            ->willReturn([]);

        $this->kafkaConsumer->offsetsForTimes([], 1000);
    }
}
