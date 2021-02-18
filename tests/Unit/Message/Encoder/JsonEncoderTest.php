<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message\Encoder;

use Jobcloud\Kafka\Message\Encoder\JsonEncoder;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Encoder\JsonEncoder
 */
class JsonEncoderTest extends TestCase
{
    /**
     * @return void
     */
    public function testEncode(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(['name' => 'foo']);
        $message->expects(self::once())->method('withBody')->with('{"name":"foo"}')->willReturn($message);

        $encoder = $this->getMockForAbstractClass(JsonEncoder::class);

        self::assertSame($message, $encoder->encode($message));
    }

    /**
     * @return void
     */
    public function testEncodeThrowsException(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(chr(255));

        $encoder = $this->getMockForAbstractClass(JsonEncoder::class);

        self::expectException(\JsonException::class);

        $encoder->encode($message);
    }
}
