<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumer;
use Jobcloud\Messaging\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerAssignmentException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerRequestException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\AbstractKafkaConsumer
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumer
 */
final class KafkaHighLevelConsumerTest extends TestCase
{

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccess(): void
    {
        $topics = [new TopicSubscription('testTopic')];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::at(0))->method('getTopicSubscriptions')->willReturn($topics);
        $kafkaConfigurationMock->expects(self::at(1))->method('getTopicSubscriptions')->willReturn([]);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('subscribe')->with(['testTopic']);

        $kafkaConsumer->subscribe($topics);
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccessWithAssignment(): void
    {
        $topics = [new TopicSubscription('testTopic', [1,2], RD_KAFKA_OFFSET_BEGINNING)];
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::at(0))->method('getTopicSubscriptions')->willReturn([]);
        $kafkaConfigurationMock->expects(self::at(1))->method('getTopicSubscriptions')->willReturn($topics);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $rdKafkaConsumerMock->expects(self::once())->method('assign');

        $kafkaConsumer->subscribe($topics);
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

        $kafkaConsumer->subscribe($topics);
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
        $message->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $message->expects(self::exactly(2))->method('getPartition')->willReturn(1);
        $message2 = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message2->expects(self::exactly(2))->method('getOffset')->willReturn(1);
        $message2->expects(self::exactly(1))->method('getTopicName')->willReturn('test');
        $message2->expects(self::exactly(1))->method('getPartition')->willReturn(1);


        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);
        $rdKafkaConsumerMock->expects(self::once())->method('commit');

        $kafkaConsumer->commit([$message, $message2]);
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

    public function testKafkaConsume(): void
    {
        $message = new Message();
        $message->key = 'test';
        $message->payload = 'test';
        $message->topic_name = 'test';
        $message->partition = 9;
        $message->offset = 500;
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
            ->with(0)
            ->willReturn($message);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::at(0))->method('getTopicSubscriptions')->willReturn($topics);
        $kafkaConfigurationMock->expects(self::at(1))->method('getTopicSubscriptions')->willReturn([]);
        $kafkaConfigurationMock->expects(self::once())->method('getTimeout')->willReturn(0);
        $decoderMock = $this->getMockForAbstractClass(DecoderInterface::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock, $decoderMock);

        $kafkaConsumer->subscribe();
        $kafkaConsumer->consume();
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
}
