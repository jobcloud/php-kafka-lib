<?php

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Producer;

use Jobcloud\Kafka\Exception\KafkaProducerTransactionAbortException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionFatalException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionRetryException;
use Jobcloud\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Kafka\Exception\KafkaProducerException;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Producer\KafkaProducer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use RdKafka\Metadata as RdKafkaMetadata;
use RdKafka\Metadata\Collection as RdKafkaMetadataCollection;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\KafkaErrorException as RdKafkaErrorException;

/**
 * @covers \Jobcloud\Kafka\Producer\KafkaProducer
 */
class KafkaProducerTest extends TestCase
{

    /**
     * @var KafkaConfiguration|MockObject
     */
    private $kafkaConfigurationMock;

    /**
     * @var RdKafkaProducer|MockObject
     */
    private $rdKafkaProducerMock;

    /**
     * @var EncoderInterface|MockObject
     */
    private $encoderMock;

    /**
     * @var KafkaProducer
     */
    private $kafkaProducer;

    public function setUp(): void
    {
        $this->kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $this->rdKafkaProducerMock = $this->createMock(RdKafkaProducer::class);
        $this->encoderMock = $this->getMockForAbstractClass(EncoderInterface::class);
        $this->kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $this->encoderMock);
    }

    /**
     * @return void
     * @throws KafkaProducerException
     */
    public function testProduceError(): void
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        $this->encoderMock->expects(self::once())->method('encode')->willReturn($message);

        self::expectException(KafkaProducerException::class);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                RD_KAFKA_MSG_F_BLOCK,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            )
            ->willThrowException(new KafkaProducerException());

        $this->rdKafkaProducerMock
            ->expects(self::any())
            ->method('newTopic')
            ->willReturn($rdKafkaProducerTopicMock);

        $this->kafkaProducer->produce($message);
    }

    public function testProduceSuccess()
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                RD_KAFKA_MSG_F_BLOCK,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            );

        $this->encoderMock
            ->expects(self::once())
            ->method('encode')
            ->with($message)
            ->willReturn($message);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('poll')
            ->with(0);

        $this->kafkaProducer->produce($message);
    }

    public function testSyncProduceSuccess()
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                RD_KAFKA_MSG_F_BLOCK,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            );

        $this->encoderMock
            ->expects(self::once())
            ->method('encode')
            ->with($message)
            ->willReturn($message);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('poll')
            ->with(-1);

        $this->kafkaProducer->syncProduce($message);
    }

    public function testPoll()
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('poll')
            ->with(1000);

        $this->kafkaProducer->poll(1000);
    }

    public function testPollDefault()
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('poll')
            ->with(0);

        $this->kafkaProducer->poll();
    }

    public function testPollUntilQueueSizeReached()
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                RD_KAFKA_MSG_F_BLOCK,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            );

        $this->rdKafkaProducerMock
            ->expects(self::exactly(3))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                        case 1:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );
        $this->encoderMock
            ->expects(self::once())
            ->method('encode')
            ->with($message)
            ->willReturn($message);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(0);

        $this->kafkaProducer->produce($message, false);
        $this->kafkaProducer->pollUntilQueueSizeReached();
    }

    /**
     * @return void
     */
    public function testPurge(): void
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('purge')
            ->with(RD_KAFKA_PURGE_F_QUEUE)
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        $this->kafkaProducer->purge(RD_KAFKA_PURGE_F_QUEUE);
    }

    /**
     * @return void
     */
    public function testFlush(): void
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('flush')
            ->with(100)
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        $this->kafkaProducer->flush(100);
    }

    /**
     * @return void
     */
    public function testGetMetadataForTopic(): void
    {
        $topicMock = $this->createMock(RdKafkaProducerTopic::class);
        $metadataMock = $this->createMock(RdKafkaMetadata::class);
        $metadataCollectionMock = $this->createMock(RdKafkaMetadataCollection::class);
        $metadataTopic = $this->createMock(RdKafkaMetadataTopic::class);
        $metadataMock
            ->expects(self::once())
            ->method('getTopics')
            ->willReturn($metadataCollectionMock);
        $metadataCollectionMock
            ->expects(self::once())
            ->method('current')
            ->willReturn($metadataTopic);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic-name')
            ->willReturn($topicMock);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $topicMock, 1000)
            ->willReturn($metadataMock);
        $this->kafkaProducer->getMetadataForTopic('test-topic-name', 1000);
    }

    /**
     * @return void
     */
    public function testGetMetadataForTopicDefault(): void
    {
        $topicMock = $this->createMock(RdKafkaProducerTopic::class);
        $metadataMock = $this->createMock(RdKafkaMetadata::class);
        $metadataCollectionMock = $this->createMock(RdKafkaMetadataCollection::class);
        $metadataTopic = $this->createMock(RdKafkaMetadataTopic::class);
        $metadataMock
            ->expects(self::once())
            ->method('getTopics')
            ->willReturn($metadataCollectionMock);
        $metadataCollectionMock
            ->expects(self::once())
            ->method('current')
            ->willReturn($metadataTopic);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic-name')
            ->willReturn($topicMock);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $topicMock, 10000)
            ->willReturn($metadataMock);
        $this->kafkaProducer->getMetadataForTopic('test-topic-name');
    }

    /**
     * @return void
     */
    public function testBeginTransactionSuccess(): void
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('initTransactions')
            ->with(10000)
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('beginTransaction')
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        self::assertNull($this->kafkaProducer->beginTransaction(10000));
    }

    /**
     * @return void
     */
    public function testBeginTransactionConsecutiveSuccess(): void
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('initTransactions')
            ->with(10000)
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('beginTransaction')
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        self::assertNull($this->kafkaProducer->beginTransaction(10000));
        self::assertNull($this->kafkaProducer->beginTransaction(10000));

    }

    /**
     * @return void
     */
    public function testBeginTransactionWithRetriableError(): void
    {
        self::expectException(KafkaProducerTransactionRetryException::class);
        self::expectExceptionMessage(
            sprintf(KafkaProducerTransactionRetryException::RETRIABLE_TRANSACTION_EXCEPTION_MESSAGE, '')
        );

        $errorMock = $this->createMock(RdKafkaErrorException::class);
        $errorMock->expects(self::once())->method('isRetriable')->willReturn(true);

        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('initTransactions')
            ->with(10000)
            ->willThrowException($errorMock);

        $this->rdKafkaProducerMock->expects(self::never())->method('beginTransaction');

        self::assertNull($this->kafkaProducer->beginTransaction(10000));
    }

    /**
     * @return void
     */
    public function testBeginTransactionWithAbortError(): void
    {
        self::expectException(KafkaProducerTransactionAbortException::class);
        self::expectExceptionMessage(
            sprintf(KafkaProducerTransactionAbortException::TRANSACTION_REQUIRES_ABORT_EXCEPTION_MESSAGE, '')
        );

        $errorMock = $this->createMock(RdKafkaErrorException::class);
        $errorMock->expects(self::once())->method('isRetriable')->willReturn(false);
        $errorMock->expects(self::once())->method('transactionRequiresAbort')->willReturn(true);

        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('initTransactions')
            ->with(10000)
            ->willThrowException($errorMock);

        $this->rdKafkaProducerMock->expects(self::never())->method('beginTransaction');

        self::assertNull($this->kafkaProducer->beginTransaction(10000));
    }

    /**
     * @return void
     */
    public function testBeginTransactionWithFatalError(): void
    {
        self::expectException(KafkaProducerTransactionFatalException::class);
        self::expectExceptionMessage(
            sprintf(KafkaProducerTransactionFatalException::FATAL_TRANSACTION_EXCEPTION_MESSAGE, '')
        );

        $errorMock = $this->createMock(RdKafkaErrorException::class);
        $errorMock->expects(self::once())->method('isRetriable')->willReturn(false);
        $errorMock->expects(self::once())->method('transactionRequiresAbort')->willReturn(false);

        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('initTransactions')
            ->with(10000)
            ->willThrowException($errorMock);

        $this->rdKafkaProducerMock->expects(self::never())->method('beginTransaction');

        self::assertNull($this->kafkaProducer->beginTransaction(10000));
    }

    /**
     * @return void
     */
    public function testBeginTransactionWithFatalErrorWillTriggerInit(): void
    {
        $firstExceptionCaught = false;

        self::expectException(KafkaProducerTransactionFatalException::class);
        self::expectExceptionMessage(
            sprintf(KafkaProducerTransactionFatalException::FATAL_TRANSACTION_EXCEPTION_MESSAGE, '')
        );

        $errorMock = $this->createMock(RdKafkaErrorException::class);
        $errorMock->expects(self::exactly(2))->method('isRetriable')->willReturn(false);
        $errorMock->expects(self::exactly(2))->method('transactionRequiresAbort')->willReturn(false);

        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('initTransactions')
            ->with(10000)
            ->willThrowException($errorMock);

        $this->rdKafkaProducerMock->expects(self::never())->method('beginTransaction');

        try {
            self::assertNull($this->kafkaProducer->beginTransaction(10000));
        } catch (KafkaProducerTransactionFatalException $e) {
            $firstExceptionCaught = true;
        }

        self::assertTrue($firstExceptionCaught);
        self::assertNull($this->kafkaProducer->beginTransaction(10000));
    }

    /**
     * @return void
     */
    public function testAbortTransactionSuccess(): void
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('abortTransaction')
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        self::assertNull($this->kafkaProducer->abortTransaction(10000));
    }

    /**
     * @return void
     */
    public function testAbortTransactionFailure(): void
    {
        self::expectException(KafkaProducerTransactionRetryException::class);
        self::expectExceptionMessage(
            sprintf(KafkaProducerTransactionRetryException::RETRIABLE_TRANSACTION_EXCEPTION_MESSAGE, 'test')
        );

        $exception = new RdKafkaErrorException('test', 1, 'some failure', false, true, false);

        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('abortTransaction')
            ->willThrowException($exception);

        $this->kafkaProducer->abortTransaction(10000);
    }

    /**
     * @return void
     */
    public function testCommitTransactionSuccess(): void
    {
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('commitTransaction')
            ->with(10000)
            ->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        self::assertNull($this->kafkaProducer->commitTransaction(10000));
    }

    /**
     * @return void
     */
    public function testCommitTransactionFailure(): void
    {
        self::expectException(KafkaProducerTransactionRetryException::class);
        self::expectExceptionMessage(
            sprintf(KafkaProducerTransactionRetryException::RETRIABLE_TRANSACTION_EXCEPTION_MESSAGE, 'test')
        );

        $exception = new RdKafkaErrorException('test', 1, 'some failure', false, true, false);

        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('commitTransaction')
            ->with(10000)
            ->willThrowException($exception);

        $this->kafkaProducer->commitTransaction(10000);
    }

    /**
     * @return void
     */
    public function testCommitTransactionFailurePreviousException(): void
    {
        $exception = new RdKafkaErrorException('test', 1, 'some failure', false, true, false);

        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('commitTransaction')
            ->with(10000)
            ->willThrowException($exception);

        try {
            $this->kafkaProducer->commitTransaction(10000);
        } catch (KafkaProducerTransactionRetryException $e) {
            self::assertSame($exception, $e->getPrevious());
        }
    }
}
