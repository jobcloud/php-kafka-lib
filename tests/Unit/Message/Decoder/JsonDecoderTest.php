<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Message\Decoder;

use Jobcloud\Kafka\Message\Decoder\JsonDecoder;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use JsonException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Decoder\JsonDecoder
 */
class JsonDecoderTest extends TestCase
{

    /**
     * @throws JsonException
     */
    public function testDecode(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn('{"name":"foo"}');

        $result = (new JsonDecoder())->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertEquals(['name' => 'foo'], $result->getBody());
    }

    public function testDecodeNonJson(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn('test');

        $this->expectException(JsonException::class);

        (new JsonDecoder())->decode($message);
    }
}
