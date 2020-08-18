<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message\Decoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Kafka\Message\Decoder\AvroDecoder;
use Jobcloud\Kafka\Message\Decoder\AvroDecoderInterface;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Decoder\AvroDecoder
 */
class AvroDecoderTest extends TestCase
{
    public function testDecodeTombstone()
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(null);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::never())->method('hasBodySchemaForTopic');
        $registry->expects(self::never())->method('hasKeySchemaForTopic');

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::never())->method('decodeMessage');

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertNull($result->getBody());
    }

    public function testDecodeWithSchema()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(2))->method('getDefinition')->willReturn($schemaDefinition);

        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(3))->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::once())->method('getPartition')->willReturn(0);
        $message->expects(self::once())->method('getOffset')->willReturn(1);
        $message->expects(self::once())->method('getTimestamp')->willReturn(time());
        $message->expects(self::exactly(2))->method('getKey')->willReturn('test-key');
        $message->expects(self::exactly(2))->method('getBody')->willReturn('body');
        $message->expects(self::once())->method('getHeaders')->willReturn([]);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('getKeySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(true);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::exactly(2))
            ->method('decodeMessage')
            ->withConsecutive(
                [$message->getKey(), $schemaDefinition],
                [$message->getBody(), $schemaDefinition],
            )
            ->willReturnOnConsecutiveCalls('decoded-key', ['test']);

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertSame(['test'], $result->getBody());
        self::assertSame('decoded-key', $result->getKey());

    }

    public function testDecodeKeyMode()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::once())->method('getDefinition')->willReturn($schemaDefinition);

        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(3))->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::once())->method('getPartition')->willReturn(0);
        $message->expects(self::once())->method('getOffset')->willReturn(1);
        $message->expects(self::once())->method('getTimestamp')->willReturn(time());
        $message->expects(self::exactly(2))->method('getKey')->willReturn('test-key');
        $message->expects(self::once())->method('getBody')->willReturn('body');
        $message->expects(self::once())->method('getHeaders')->willReturn([]);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::never())->method('getBodySchemaForTopic');
        $registry->expects(self::once())->method('getKeySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(false);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(true);


        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('decodeMessage')->with($message->getKey(), $schemaDefinition)->willReturn('decoded-key');

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertSame('decoded-key', $result->getKey());
        self::assertSame('body', $result->getBody());

    }

    public function testDecodeBodyMode()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::once())->method('getDefinition')->willReturn($schemaDefinition);

        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(3))->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::once())->method('getPartition')->willReturn(0);
        $message->expects(self::once())->method('getOffset')->willReturn(1);
        $message->expects(self::once())->method('getTimestamp')->willReturn(time());
        $message->expects(self::once())->method('getKey')->willReturn('test-key');
        $message->expects(self::exactly(2))->method('getBody')->willReturn('body');
        $message->expects(self::once())->method('getHeaders')->willReturn([]);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::never())->method('getKeySchemaForTopic');
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(false);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('decodeMessage')->with($message->getBody(), $schemaDefinition)->willReturn(['test']);

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertSame('test-key', $result->getKey());
        self::assertSame(['test'], $result->getBody());

    }

    public function testGetRegistry()
    {
        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();

        $decoder = new AvroDecoder($registry, $recordSerializer);

        self::assertSame($registry, $decoder->getRegistry());
    }
}
