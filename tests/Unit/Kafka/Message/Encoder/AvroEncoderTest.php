<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Encoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Messaging\Kafka\Exception\AvroEncoderException;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use PHPStan\Testing\TestCase;
use \AvroSchema;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder
 */
class AvroEncoderTest extends TestCase
{
    public function testEncodeTombstone()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getBody')->willReturn(null);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::never())->method('encodeRecord');
        $encoder = new AvroEncoder($registry, $recordSerializer);
        $result = $encoder->encode($producerMessage);

        self::assertSame($producerMessage, $result);
    }

    public function testEncodeWithoutSchema()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(3))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn('test');


        self::expectException(AvroEncoderException::class);
        self::expectExceptionMessage(
            sprintf(
                AvroEncoderException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                $producerMessage->getTopicName()
            )
        );

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $encoder = new AvroEncoder($registry, $recordSerializer);
        $encoder->encode($producerMessage);
    }

    public function testEncodeWithoutSchemaDefinition()
    {
        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::once())->method('getDefinition')->willReturn(null);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once(1))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn('test');

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getSchemaForTopic')->willReturn($avroSchema);

        self::expectException(AvroEncoderException::class);
        self::expectExceptionMessage(
            sprintf(
                AvroEncoderException::UNABLE_TO_LOAD_DEFINITION_MESSAGE,
                $avroSchema->getName()
            )
        );

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();

        $encoder = new AvroEncoder($registry, $recordSerializer);
        $encoder->encode($producerMessage);
    }

    public function testEncodeSuccessWithSchema()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(2))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::never())->method('getVersion');
        $avroSchema->expects(self::exactly(3))->method('getDefinition')->willReturn($schemaDefinition);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getSchemaForTopic')->willReturn($avroSchema);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn([]);
        $producerMessage->expects(self::once())->method('withBody')->with('encodedValue')->willReturn($producerMessage);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('encodeRecord')->with($avroSchema->getName(), $avroSchema->getDefinition(), [])->willReturn('encodedValue');

        $encoder = new AvroEncoder($registry, $recordSerializer);

        self::assertSame($producerMessage, $encoder->encode($producerMessage));
    }

    public function testGetRegistry()
    {
        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $encoder = new AvroEncoder($registry, $recordSerializer);

        self::assertSame($registry, $encoder->getRegistry());
    }
}
