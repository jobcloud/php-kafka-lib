<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Conf;

use Jobcloud\Kafka\Consumer\TopicSubscription;
use RdKafka\Conf as RdKafkaConf;

class KafkaConfiguration extends RdKafkaConf
{

    /**
     * @var array<string>
     */
    protected $brokers;

    /**
     * @var array|TopicSubscription[]
     */
    protected $topicSubscriptions;

    /**
     * @var int
     */
    protected $timeout;

    /**
     * @param array<string> $brokers
     * @param array|TopicSubscription[] $topicSubscriptions
     * @param integer $timeout
     * @param array<mixed> $config
     */
    public function __construct(array $brokers, array $topicSubscriptions, int $timeout, array $config = [])
    {
        parent::__construct();

        $this->brokers = $brokers;
        $this->topicSubscriptions = $topicSubscriptions;
        $this->timeout = $timeout;

        $this->initializeConfig($config);
    }

    /**
     * @return array<string>
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return array|TopicSubscription[]
     */
    public function getTopicSubscriptions(): array
    {
        return $this->topicSubscriptions;
    }

    /**
     * @return integer
     */
    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * @return array<string>
     */
    public function getConfiguration(): array
    {
        return $this->dump();
    }

    /**
     * @param array<mixed> $config
     * @return void
     */
    protected function initializeConfig(array $config = []): void
    {

        foreach ($config as $name => $value) {
            if (false === is_scalar($value)) {
                continue;
            }

            if (true === is_bool($value)) {
                $value = true === $value ? 'true' : 'false';
            }

            $this->set($name, (string) $value);
        }

        $this->set('metadata.broker.list', implode(',', $this->getBrokers()));
    }
}
