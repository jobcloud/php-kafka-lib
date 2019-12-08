<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message\Encoder;

use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Kafka\Message\Encoder\NullEncoder;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Encoder\NullEncoder
 */
class NullEncoderTest extends TestCase
{

    /**
     * @return void
     */
    public function testEncode(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);

        $this->assertSame($message, (new NullEncoder())->encode($message));
    }
}
