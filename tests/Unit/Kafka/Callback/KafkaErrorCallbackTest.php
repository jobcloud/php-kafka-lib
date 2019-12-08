<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;

use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;

/**
 * @covers \Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback
 */
class KafkaErrorCallbackTest extends TestCase
{

    public function getConsumerMock()
    {
        return $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['unsubscribe', 'getSubscription'])
            ->getMock();
    }

    public function testInvokeWithBrokerException()
    {
        self::expectException('Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException');

        $consumerMock = $this->getConsumerMock();

        $consumerMock
            ->expects(self::any())
            ->method('unsubscribe')
            ->willReturn(null);

        $consumerMock
            ->expects(self::any())
            ->method('getSubscription')
            ->willReturn([]);

        $callback = new KafkaErrorCallback();
        call_user_func($callback, $consumerMock, 1, 'error');
    }

    public function testInvokeWithAcceptableError()
    {
        $consumerMock = $this->getConsumerMock();

        $consumerMock
            ->expects(self::any())
            ->method('unsubscribe')
            ->willReturn(null);

        $consumerMock
            ->expects(self::any())
            ->method('getSubscription')
            ->willReturn([]);

        $callback = new KafkaErrorCallback();
        $result = call_user_func($callback, $consumerMock, RD_KAFKA_RESP_ERR__TRANSPORT, 'error');

        self::assertNull($result);
    }
}
