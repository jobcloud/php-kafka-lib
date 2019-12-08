<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Decoder;

use Jobcloud\Messaging\Kafka\Message\Decoder\NullDecoder;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Decoder\NullDecoder
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
