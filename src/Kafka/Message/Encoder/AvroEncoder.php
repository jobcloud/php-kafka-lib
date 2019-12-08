<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Encoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroEncoderException;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;

final class AvroEncoder implements AvroEncoderInterface
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
     * @param AvroSchemaRegistryInterface $registry
     * @param RecordSerializer            $recordSerializer
     */
    public function __construct(AvroSchemaRegistryInterface $registry, RecordSerializer $recordSerializer)
    {
        $this->recordSerializer = $recordSerializer;
        $this->registry = $registry;
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws AvroEncoderException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        if (null === $producerMessage->getBody()) {
            return $producerMessage;
        }

        if (null === $avroSchema = $this->registry->getSchemaForTopic($producerMessage->getTopicName())) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                    $producerMessage->getTopicName()
                )
            );
        }

        if (null === $avroSchema->getDefinition()) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::UNABLE_TO_LOAD_DEFINITION_MESSAGE,
                    $avroSchema->getName()
                )
            );
        }

        $body = $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $avroSchema->getDefinition(),
            $producerMessage->getBody()
        );

        return $producerMessage->withBody($body);
    }

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
