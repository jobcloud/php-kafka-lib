<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

final class AvroDecoder implements AvroDecoderInterface
{
    /**
     * @var AvroSchemaRegistryInterface
     */
    private $registry;

    /**
     * @var RecordSerializer
     */
    private $recordSerializer;

    /**
     * @var string
     */
    private $decodeMode;

    /**
     * @param AvroSchemaRegistryInterface $registry
     * @param RecordSerializer            $recordSerializer
     * @param string                      $decodeMode
     */
    public function __construct(
        AvroSchemaRegistryInterface $registry,
        RecordSerializer $recordSerializer,
        string $decodeMode = self::DECODE_ALL
    ) {
        $this->recordSerializer = $recordSerializer;
        $this->registry = $registry;
        $this->decodeMode = $decodeMode;
    }

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     * @throws SchemaRegistryException
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        return new KafkaConsumerMessage(
            $consumerMessage->getTopicName(),
            $consumerMessage->getPartition(),
            $consumerMessage->getOffset(),
            $consumerMessage->getTimestamp(),
            $this->decodeKey($consumerMessage),
            $this->decodeBody($consumerMessage),
            $consumerMessage->getHeaders()
        );
    }

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return mixed
     * @throws SchemaRegistryException
     */
    private function decodeBody(KafkaConsumerMessageInterface $consumerMessage)
    {
        $schemaDefinition = null;

        if (self::DECODE_KEY === $this->decodeMode) {
            return $consumerMessage->getBody();
        }

        if (null === $consumerMessage->getBody()) {
            return null;
        }

        $avroSchema = $this->registry->getBodySchemaForTopic($consumerMessage->getTopicName());
        $schemaDefinition = $avroSchema->getDefinition();

        return $this->recordSerializer->decodeMessage($consumerMessage->getBody(), $schemaDefinition);
    }

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return mixed
     * @throws SchemaRegistryException
     */
    private function decodeKey(KafkaConsumerMessageInterface $consumerMessage)
    {
        $schemaDefinition = null;
        $key = $consumerMessage->getKey();

        if (self::DECODE_BODY === $this->decodeMode) {
            return $key;
        }

        if (null === $key) {
            return null;
        }

        $avroSchema = $this->registry->getKeySchemaForTopic($consumerMessage->getTopicName());
        $schemaDefinition = $avroSchema->getDefinition();

        return $this->recordSerializer->decodeMessage($key, $schemaDefinition);
    }

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
