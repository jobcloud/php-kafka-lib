<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Message\Encoder;

use Jobcloud\Kafka\Message\Encoder\JsonEncoder;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use JsonException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Encoder\JsonEncoder
 */
class JsonEncoderTest extends TestCase
{
    /**
     * @throws JsonException
     */
    public function testEncode(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(['name' => 'foo']);
        $message->expects(self::once())->method('withBody')->with('{"name":"foo"}')->willReturn($message);

        self::assertSame($message, (new JsonEncoder())->encode($message));
    }

    public function testEncodeThrowsException(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(chr(255));

        $this->expectException(JsonException::class);

        (new JsonEncoder())->encode($message);
    }
}
