<?php

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Conf;

use Jobcloud\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Kafka\Consumer\TopicSubscription;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use PHPUnit\Framework\TestCase;
use stdClass;

/**
 * @covers \Jobcloud\Kafka\Conf\KafkaConfiguration
 */
class KafkaConfigurationTest extends TestCase
{

    /**
     * @return void
     */
    public function testInstance(): void
    {
        self::assertInstanceOf(KafkaConfiguration::class, new KafkaConfiguration([], []));
    }

    /**
     * @return array
     */
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
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGettersAndSetters(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions);

        self::assertEquals($brokers, $kafkaConfiguration->getBrokers());
        self::assertEquals($topicSubscriptions, $kafkaConfiguration->getTopicSubscriptions());
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetConfiguration(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions);

        self::assertEquals($kafkaConfiguration->dump(), $kafkaConfiguration->getConfiguration());
    }

    /**
     * @return array
     */
    public function configValuesProvider(): array
    {
        return [
            [ 1, '1' ],
            [ -1, '-1' ],
            [ 1.123333, '1.123333' ],
            [ -0.99999, '-0.99999' ],
            [ true, 'true' ],
            [ false, 'false' ],
            [ null, '' ],
            [ '', '' ],
            [ '  ', '  ' ],
            [ [], null ],
            [ new stdClass(), null ],
        ];
    }

    /**
     * @dataProvider configValuesProvider
     * @param mixed $inputValue
     * @param mixed $expectedValue
     */
    public function testConfigValues($inputValue, $expectedValue): void
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

        if(null === $expectedValue) {
            self::assertArrayNotHasKey('group.id', $config);
            return;
        }

        self::assertEquals($config['metadata.broker.list'], 'localhost');
        self::assertEquals($expectedValue, $config['group.id']);
        self::assertEquals('100', $config['auto.commit.interval.ms']);
        self::assertArrayHasKey('default_topic_conf', $config);
        self::assertIsString($config['default_topic_conf']);
    }
}
