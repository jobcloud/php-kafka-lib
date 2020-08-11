<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message;

use Jobcloud\Kafka\Message\KafkaProducerMessage;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\AbstractKafkaMessage
 * @covers \Jobcloud\Kafka\Message\KafkaProducerMessage
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

    public function testClone()
    {
        $key = '1234-1234-1234';
        $body = 'foo bar baz';
        $topic = 'test';
        $partition = 1;
        $headers = [ 'key' => 'value' ];


        $origMessage = KafkaProducerMessage::create($topic, $partition);

        $message = $origMessage->withKey($key);
        self::assertNotSame($origMessage, $message);

        $message = $origMessage->withBody($body);
        self::assertNotSame($origMessage, $message);

        $message = $origMessage->withHeaders($headers);
        self::assertNotSame($origMessage, $message);

        $message = $origMessage->withHeader('anotherKey', 1);
        self::assertNotSame($origMessage, $message);
    }
}
