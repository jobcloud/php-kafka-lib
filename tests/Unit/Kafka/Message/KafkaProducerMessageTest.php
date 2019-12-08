<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\AbstractKafkaMessage
 * @covers \Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage
 */
final class KafkaProducerMessageTest extends TestCase
{
    public function testMessageGettersAndConstructor()
    {
        $key = '1234-1234-1234';
        $body = 'foo bar baz';
        $topic = 'test';
        $partition = 1;
        $headers = [ 'key' => 'value' ];
        $expectedHeader = [
            'key' => 'value',
            'anotherKey' => 1
        ];

        $message = KafkaProducerMessage::create($topic, $partition)
            ->withKey($key)
            ->withBody($body)
            ->withHeaders($headers)
            ->withHeader('anotherKey', 1);

        self::assertEquals($key, $message->getKey());
        self::assertEquals($body, $message->getBody());
        self::assertEquals($topic, $message->getTopicName());
        self::assertEquals($partition, $message->getPartition());
        self::assertEquals($expectedHeader, $message->getHeaders());
    }
}
