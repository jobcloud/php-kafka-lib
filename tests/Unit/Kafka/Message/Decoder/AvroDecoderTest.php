<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Decoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Messaging\Kafka\Message\Decoder\AvroDecoder;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Decoder\AvroDecoder
 */
class AvroDecoderTest extends TestCase
{
    public function testDecodeTombstone()
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(null);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::never())->method('decodeMessage');

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertSame($message, $result);
    }

    public function testDecodeWithoutSchema()
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(2))->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::once())->method('getPartition')->willReturn(0);
        $message->expects(self::once())->method('getOffset')->willReturn(1);
        $message->expects(self::once())->method('getTimestamp')->willReturn(time());
        $message->expects(self::once())->method('getKey')->willReturn('test-key');
        $message->expects(self::exactly(3))->method('getBody')->willReturn('body');
        $message->expects(self::once())->method('getHeaders')->willReturn([]);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getSchemaForTopic')->willReturn(null);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('decodeMessage')->with($message->getBody(), null)->willReturn(['test']);

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertSame([ 0 => 'test'], $result->getBody());
    }

    public function testDecodeWithSchema()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::once())->method('getDefinition')->willReturn($schemaDefinition);

        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(2))->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::once())->method('getPartition')->willReturn(0);
        $message->expects(self::once())->method('getOffset')->willReturn(1);
        $message->expects(self::once())->method('getTimestamp')->willReturn(time());
        $message->expects(self::once())->method('getKey')->willReturn('test-key');
        $message->expects(self::exactly(3))->method('getBody')->willReturn('body');
        $message->expects(self::once())->method('getHeaders')->willReturn([]);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getSchemaForTopic')->willReturn($avroSchema);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('decodeMessage')->with($message->getBody(), $schemaDefinition)->willReturn(['test']);

        $decoder = new AvroDecoder($registry, $recordSerializer);

        $result = $decoder->decode($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertSame([ 0 => 'test'], $result->getBody());
    }

    public function testGetRegistry()
    {
        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();

        $decoder = new AvroDecoder($registry, $recordSerializer);

        self::assertSame($registry, $decoder->getRegistry());
    }
}
