<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducer
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

    /**
     * @return void
     * @throws KafkaProducerException
     */
    public function testProduceErrorOnMessageInterface(): void
    {
        self::expectException(KafkaProducerException::class);
        self::expectExceptionMessage(
            sprintf(
                KafkaProducerException::UNSUPPORTED_MESSAGE_EXCEPTION_MESSAGE,
                KafkaProducerMessageInterface::class
            )
        );

        $message = $this->createMock(MessageInterface::class);

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

        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
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
            ->with(1000);

        $this->kafkaProducer->produce($message);
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
}
