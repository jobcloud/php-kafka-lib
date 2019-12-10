<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message\Decoder;

use Jobcloud\Kafka\Message\Decoder\NullDecoder;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Decoder\NullDecoder
 */
class NullDecoderTest extends TestCase
{

    /**
     * @return void
     */
    public function testDecode(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);

        self::assertSame($message, (new NullDecoder())->decode($message));
    }
}
