<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducerInterface;
use Jobcloud\Messaging\Producer\ProducerInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder
 */
class KafkaProducerBuilderTest extends TestCase
{

    /** @var $kafkaProducerBuilder KafkaProducerBuilder */
    protected $kafkaProducerBuilder;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->kafkaProducerBuilder = KafkaProducerBuilder::create();
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testAddConfig(): void
    {
        $config = ['timeout' => 1000];
        $clone = $this->kafkaProducerBuilder->withAdditionalConfig($config);
        $config = ['timeout' => 1001];
        $clone = $clone->withAdditionalConfig($config);

        $reflectionProperty = new \ReflectionProperty($clone, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame($config, $reflectionProperty->getValue($clone));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testAddBroker(): void
    {
        $clone = $this->kafkaProducerBuilder->withAdditionalBroker('localhost');

        $reflectionProperty = new \ReflectionProperty($clone, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['localhost'], $reflectionProperty->getValue($clone));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetEncoder(): void
    {
        $encoder = $this->getMockForAbstractClass(EncoderInterface::class);

        $clone = $this->kafkaProducerBuilder->withEncoder($encoder);

        $reflectionProperty = new \ReflectionProperty($clone, 'encoder');
        $reflectionProperty->setAccessible(true);

        self::assertInstanceOf(EncoderInterface::class, $reflectionProperty->getValue($clone));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetDeliveryReportCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $clone = $this->kafkaProducerBuilder->withDeliveryReportCallback($callback);

        $reflectionProperty = new \ReflectionProperty($clone, 'deliverReportCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($clone));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetErrorCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $clone = $this->kafkaProducerBuilder->withErrorCallback($callback);

        $reflectionProperty = new \ReflectionProperty($clone, 'errorCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($clone));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetPollTimeout(): void
    {
        $clone = $this->kafkaProducerBuilder->withPollTimeout(1000);

        $reflectionProperty = new \ReflectionProperty($clone, 'pollTimeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame(1000, $reflectionProperty->getValue($clone));
    }

    /**
     * @throws KafkaProducerException
     */
    public function testBuildNoBroker(): void
    {
        self::expectException(KafkaProducerException::class);

        $this->kafkaProducerBuilder->build();
    }

    /**
     * @return void
     */
    public function testBuild(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        $producer = $this->kafkaProducerBuilder
            ->withAdditionalBroker('localhost')
            ->withDeliveryReportCallback($callback)
            ->withErrorCallback($callback)
            ->build();

        self::assertInstanceOf(ProducerInterface::class, $producer);
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testKafkaProducerBuilderConfig(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        $this->kafkaProducerBuilder
            ->withAdditionalBroker('localhost')
            ->withDeliveryReportCallback($callback)
            ->withErrorCallback($callback)
            ->build();

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame(
            [
                'socket.timeout.ms' => '50',
                'internal.termination.signal' => (string) SIGIO
            ],
            $reflectionProperty->getValue($this->kafkaProducerBuilder)
        );
    }
}
