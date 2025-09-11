<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Conf;

use Jobcloud\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Kafka\Consumer\TopicSubscription;
use RdKafka\Conf as RdKafkaConf;
use RdKafka\TopicConf as RdKafkaTopicConf;

class KafkaConfiguration extends RdKafkaConf
{
    /**
     * @var array<string,int>
     */
    private array $lowLevelTopicSettings = [
        'auto.commit.interval.ms' => 1,
        'auto.offset.reset' => 1,
    ];

    /**
     * @param string[]             $brokers
     * @param TopicSubscription[]  $topicSubscriptions
     * @param array<string, mixed> $config
     */
    public function __construct(
        protected array $brokers,
        protected array $topicSubscriptions = [],
        array $config = [],
        private string $type = '',
    ) {
        parent::__construct();

        $this->initializeConfig($config);
    }

    /**
     * @return string[]
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return TopicSubscription[]
     */
    public function getTopicSubscriptions(): array
    {
        return $this->topicSubscriptions;
    }

    /**
     * @return string[]
     */
    public function getConfiguration(): array
    {
        return $this->dump();
    }

    /**
     * @param array<string, mixed> $config
     */
    protected function initializeConfig(array $config = []): void
    {
        $topicConf = new RdKafkaTopicConf();

        foreach ($config as $name => $value) {
            if (false === is_scalar($value)) {
                continue;
            }

            if (
                KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL === $this->type
                && true === $this->isLowLevelTopicConfSetting($name)
            ) {
                $topicConf->set($name, (string) $value);
                $this->setDefaultTopicConf($topicConf);
            }

            if (true === is_bool($value)) {
                $value = true === $value ? 'true' : 'false';
            }

            $this->set($name, (string) $value);
        }

        $this->set('metadata.broker.list', implode(',', $this->getBrokers()));
    }

    private function isLowLevelTopicConfSetting(string $settingName): bool
    {
        return true === isset($this->lowLevelTopicSettings[$settingName]);
    }
}
