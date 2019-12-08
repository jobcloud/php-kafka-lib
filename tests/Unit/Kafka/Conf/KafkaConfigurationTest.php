<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Conf;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use PHPUnit\Framework\TestCase;
use stdClass;

/**
 * @covers \Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration
 */
class KafkaConfigurationTest extends TestCase
{

    /**
     * @return void
     */
    public function testInstance(): void
    {
        self::assertInstanceOf(KafkaConfiguration::class, new KafkaConfiguration([], [], 1000));
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
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, 1000);

        self::assertEquals($brokers, $kafkaConfiguration->getBrokers());
        self::assertEquals($topicSubscriptions, $kafkaConfiguration->getTopicSubscriptions());
        self::assertEquals(1000, $kafkaConfiguration->getTimeout());
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetConfiguration(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, 1000);

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
            1000,
            [
                'group.id' => $inputValue,
            ]
        );

        $config = $kafkaConfiguration->getConfiguration();

        if(null === $expectedValue) {
            self::assertArrayNotHasKey('group.id', $config);
            return;
        }

        self::assertEquals($expectedValue, $config['group.id']);
    }
}
