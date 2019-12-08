<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Consumer;

use Jobcloud\Kafka\Consumer\TopicSubscription;
use PHPUnit\Framework\TestCase;
use RdKafka\TopicConf;

/**
 * @covers \Jobcloud\Kafka\Consumer\TopicSubscription
 */
final class TopicSubscriptionTest extends TestCase
{
    public function testGettersAndSetters()
    {
        $topicName = 'test';
        $partitions = [1, 2];
        $offset = 1;
        $newPartitions = [2, 3];

        $topicSubscription = new TopicSubscription($topicName, $partitions, $offset);

        self::assertEquals($topicName, $topicSubscription->getTopicName());
        self::assertEquals($partitions, $topicSubscription->getPartitions());
        self::assertEquals($offset, $topicSubscription->getOffset());

        $topicSubscription->setPartitions($newPartitions);

        self::assertEquals($newPartitions, $topicSubscription->getPartitions());
    }
}
