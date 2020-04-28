<?php

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Callback;

use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Kafka\Callback\KafkaErrorCallback;

/**
 * @covers \Jobcloud\Kafka\Callback\KafkaErrorCallback
 */
class KafkaErrorCallbackTest extends TestCase
{
    public function testInvokeWithBrokerException()
    {
        self::expectException('Jobcloud\Kafka\Exception\KafkaBrokerException');
        $callback = new KafkaErrorCallback();
        call_user_func($callback, null, RD_KAFKA_RESP_ERR__FATAL, 'error');
    }

    public function testInvokeWithAcceptableError()
    {
        $callback = new KafkaErrorCallback();
        $result = call_user_func($callback, null, RD_KAFKA_RESP_ERR__TRANSPORT, 'error');

        self::assertNull($result);
    }
}
