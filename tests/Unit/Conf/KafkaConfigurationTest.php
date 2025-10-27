<?php

namespace Jobcloud\Kafka\Tests\Unit\Conf;

use Jobcloud\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Kafka\Consumer\TopicSubscription;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use stdClass;

/**
 * @covers \Jobcloud\Kafka\Conf\KafkaConfiguration
 */
class KafkaConfigurationTest extends TestCase
{

    public function testInstance(): void
    {
        self::assertInstanceOf(KafkaConfiguration::class, new KafkaConfiguration([], []));
    }

    public function kafkaConfigurationDataProvider(): array
    {
        $brokers = ['localhost'];
        $topicSubscriptions = [new TopicSubscription('test-topic')];

        return [
            [
                $brokers,
                $topicSubscriptions
            ]
        ];
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     */
    public function testGettersAndSetters(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions);

        self::assertEquals($brokers, $kafkaConfiguration->getBrokers());
        self::assertEquals($topicSubscriptions, $kafkaConfiguration->getTopicSubscriptions());
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     */
    public function testGetConfiguration(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions);

        self::assertEquals($kafkaConfiguration->dump(), $kafkaConfiguration->getConfiguration());
    }

    public function configValuesProvider(): array
    {
        return [
            [ 1, '1' ],
            [ -1, '-1' ],
            [ 1.123333, '1.123333' ],
            [ -0.99999, '-0.99999' ],
            [ true, 'true' ],
            [ false, 'false' ],
            [ '  ', '  ' ],
            [ [], null ],
            [ new stdClass(), null ],
        ];
    }

    /**
     * @dataProvider configValuesProvider
     */
    public function testConfigValues(mixed $inputValue, mixed $expectedValue): void
    {
        $kafkaConfiguration = new KafkaConfiguration(
            ['localhost'],
            [new TopicSubscription('test-topic')],
            [
                'group.id' => $inputValue,
                'auto.commit.interval.ms' => 100
            ],
            KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL
        );

        $config = $kafkaConfiguration->getConfiguration();

        if (null === $expectedValue) {
            self::assertArrayNotHasKey('group.id', $config);
            return;
        }

        self::assertEquals('localhost', $config['metadata.broker.list']);
        self::assertEquals($expectedValue, $config['group.id']);
        self::assertEquals('100', $config['auto.commit.interval.ms']);
        self::assertArrayHasKey('default_topic_conf', $config);
        self::assertIsString($config['default_topic_conf']);
    }

    public function testMethodVisibility(): void
    {
        $reflectionClass = new ReflectionClass(KafkaConfiguration::class);

        $methodInitializedConfig = $reflectionClass->getMethod('initializeConfig');
        $methodInitializedConfig->setAccessible(true);

        $this->assertTrue($methodInitializedConfig->isProtected());
    }
}
