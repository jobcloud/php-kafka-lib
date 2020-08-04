<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Conf;

use Jobcloud\Kafka\Consumer\TopicSubscription;
use RdKafka\Conf as RdKafkaConf;
use RdKafka\TopicConf as RdKafkaTopicConf;

class KafkaConfiguration extends RdKafkaConf
{

    /**
     * @var string[]
     */
    protected $brokers;

    /**
     * @var array|TopicSubscription[]
     */
    protected $topicSubscriptions;

    /**
     * @var array<string,int>
     */
    private $lowLevelTopicSettings = [
        'auto.commit.interval.ms' => 1,
        'auto.offset.reset' => 1,
    ];

    /**
     * @param string[] $brokers
     * @param array|TopicSubscription[] $topicSubscriptions
     * @param mixed[] $config
     */
    public function __construct(array $brokers, array $topicSubscriptions, array $config = [])
    {
        parent::__construct();

        $this->brokers = $brokers;
        $this->topicSubscriptions = $topicSubscriptions;

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
     * @return array|TopicSubscription[]
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
     * @param mixed[] $config
     * @return void
     */
    protected function initializeConfig(array $config = []): void
    {
        $topicConf = new RdKafkaTopicConf();

        foreach ($config as $name => $value) {
            if (false === is_scalar($value)) {
                continue;
            }

            if (true === $this->isLowLevelTopicConfSetting($name)) {
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

    private function isLowLevelTopicConfSetting(string $settingName) {
        return true === isset($this->lowLevelTopicSettings[$settingName]);
    }
}
