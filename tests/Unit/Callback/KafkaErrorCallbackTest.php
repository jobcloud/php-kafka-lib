<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Callback;

use PHPUnit\Framework\TestCase;
use Jobcloud\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Kafka\Exception\KafkaBrokerException;

/**
 * @covers \Jobcloud\Kafka\Callback\KafkaErrorCallback
 */
class KafkaErrorCallbackTest extends TestCase
{
    public function testInvokeWithBrokerException(): void
    {
        $this->expectException(KafkaBrokerException::class);

        $callback = new KafkaErrorCallback();
        call_user_func($callback, null, RD_KAFKA_RESP_ERR__FATAL, 'error');
    }

    public function testInvokeWithAcceptableError(): void
    {
        $callback = new KafkaErrorCallback();
        $result = call_user_func($callback, null, RD_KAFKA_RESP_ERR__TRANSPORT, 'error');

        self::assertNull($result);
    }
}
