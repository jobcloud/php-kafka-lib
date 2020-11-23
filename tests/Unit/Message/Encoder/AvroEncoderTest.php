<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message\Encoder;

use FlixTech\AvroSerializer\Objects\Exceptions\AvroEncodingException;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Kafka\Exception\AvroEncoderException;
use Jobcloud\Kafka\Exception\AvroValidatorException;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Kafka\Message\Encoder\AvroEncoder;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use PHPStan\Testing\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Encoder\AvroEncoder
 */
class AvroEncoderTest extends TestCase
{
    private $avroValidatorClass = "./src/Message/Encoder/AvroEncoder.php";

    private $originalNamespaces = [
        "Jobcloud\Avro\Validator\RecordRegistry",
        "Jobcloud\Avro\Validator\Validator"
    ];

    private $replacedNamespaces = [
        "Jobcloud\Kafka\Tests\Unit\Kafka\Message\Encoder\RecordRegistry",
        "Jobcloud\Kafka\Tests\Unit\Kafka\Message\Encoder\Validator"
    ];

    protected function setUp(): void
    {
        $avroEncoderContent = file_get_contents($this->avroValidatorClass);

        $avroEncoderContent = str_replace($this->originalNamespaces, $this->replacedNamespaces, $avroEncoderContent);

        file_put_contents($this->avroValidatorClass, $avroEncoderContent);
    }

    protected function tearDown(): void
    {
        $avroEncoderContent = file_get_contents($this->avroValidatorClass);

        $avroEncoderContent = str_replace($this->replacedNamespaces, $this->originalNamespaces, $avroEncoderContent);

        file_put_contents($this->avroValidatorClass, $avroEncoderContent);
    }

    public function testEncodeTombstone()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn(null);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::never())->method('hasBodySchemaForTopic');
        $registry->expects(self::never())->method('hasKeySchemaForTopic');
        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::never())->method('encodeRecord');
        $encoder = new AvroEncoder($registry, $recordSerializer);
        $result = $encoder->encode($producerMessage);

        self::assertInstanceOf(KafkaProducerMessageInterface::class, $result);
        self::assertSame($producerMessage, $result);
        self::assertNull($result->getBody());
    }

    public function testEncodeWithoutSchemaDefinition()
    {
        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::once())->method('getDefinition')->willReturn(null);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn('test');

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);

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
        $avroSchema->expects(self::exactly(4))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::never())->method('getVersion');
        $avroSchema->expects(self::exactly(4))->method('getDefinition')->willReturn($schemaDefinition);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('getKeySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(true);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn([]);
        $producerMessage->expects(self::once())->method('getKey')->willReturn('test-key');
        $producerMessage->expects(self::once())->method('withBody')->with('encodedValue')->willReturn($producerMessage);
        $producerMessage->expects(self::once())->method('withKey')->with('encodedKey')->willReturn($producerMessage);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer
            ->expects(self::exactly(2))
            ->method('encodeRecord')
            ->withConsecutive(
                [$avroSchema->getName(), $avroSchema->getDefinition(), []],
                [$avroSchema->getName(), $avroSchema->getDefinition(), 'test-key']
            )->willReturnOnConsecutiveCalls('encodedValue', 'encodedKey');

        $encoder = new AvroEncoder($registry, $recordSerializer);

        self::assertSame($producerMessage, $encoder->encode($producerMessage));
    }

    public function testEncodeKeyMode()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(2))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::never())->method('getVersion');
        $avroSchema->expects(self::exactly(2))->method('getDefinition')->willReturn($schemaDefinition);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::never())->method('getBodySchemaForTopic');
        $registry->expects(self::once())->method('getKeySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(false);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(true);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn([]);
        $producerMessage->expects(self::once())->method('getKey')->willReturn('test-key');
        $producerMessage->expects(self::never())->method('withBody');
        $producerMessage->expects(self::once())->method('withKey')->with('encodedKey')->willReturn($producerMessage);

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('encodeRecord')->with($avroSchema->getName(), $avroSchema->getDefinition(), 'test-key')->willReturn('encodedKey');

        $encoder = new AvroEncoder($registry, $recordSerializer);

        self::assertSame($producerMessage, $encoder->encode($producerMessage));
    }

    public function testEncodeBodyMode()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(2))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::never())->method('getVersion');
        $avroSchema->expects(self::exactly(2))->method('getDefinition')->willReturn($schemaDefinition);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::never())->method('getKeySchemaForTopic');
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(false);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn([]);
        $producerMessage->expects(self::once())->method('getKey')->willReturn('test-key');
        $producerMessage->expects(self::once())->method('withBody')->with('encodedBody')->willReturn($producerMessage);
        $producerMessage->expects(self::never())->method('withKey');

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer->expects(self::once())->method('encodeRecord')->with($avroSchema->getName(), $avroSchema->getDefinition(), [])->willReturn('encodedBody');

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

    public function testAvroValidatorBodyException()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::once())->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::exactly(2))->method('getDefinition')->willReturn($schemaDefinition);
        $schemaDefinition->method('to_avro')->willReturn(['type' => 'record']);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn(['id' => 123]);

        $avroEncodingException = $this->getMockBuilder(AvroEncodingException::class)
            ->disableOriginalConstructor()
            ->getMock();
        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)
            ->disableOriginalConstructor()
            ->getMock();
        $recordSerializer
            ->expects(self::once())
            ->method('encodeRecord')
            ->willReturnOnConsecutiveCalls('encodedValue')
            ->willThrowException($avroEncodingException);

        $encoder = new AvroEncoder($registry, $recordSerializer);

        self::expectException(AvroValidatorException::class);
        self::expectExceptionMessage(json_encode(['test' => 'test']));
        self::assertNotSame($producerMessage, $encoder->encode($producerMessage));
    }

    public function testAvroValidatorKeyException()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(4))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::never())->method('getVersion');
        $avroSchema->expects(self::exactly(5))->method('getDefinition')->willReturn($schemaDefinition);
        $schemaDefinition->method('to_avro')->willReturn([]);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getBodySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('getKeySchemaForTopic')->willReturn($avroSchema);
        $registry->expects(self::once())->method('hasBodySchemaForTopic')->willReturn(true);
        $registry->expects(self::once())->method('hasKeySchemaForTopic')->willReturn(true);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn([]);
        $producerMessage->expects(self::once())->method('getKey')->willReturn('test-key');
        $producerMessage->expects(self::once())->method('withBody')->with('encodedValue')->willReturn($producerMessage);

        $avroEncodingException = $this->getMockBuilder(AvroEncodingException::class)->disableOriginalConstructor()->getMock();

        $recordSerializer = $this->getMockBuilder(RecordSerializer::class)->disableOriginalConstructor()->getMock();
        $recordSerializer
            ->expects(self::exactly(2))
            ->method('encodeRecord')
            ->withConsecutive(
                [$avroSchema->getName(), $avroSchema->getDefinition(), []],
                [$avroSchema->getName(), $avroSchema->getDefinition(), 'test-key']
            )
            ->willReturnOnConsecutiveCalls('encodedValue', $this->throwException($avroEncodingException));

        $encoder = new AvroEncoder($registry, $recordSerializer);

        self::expectException(AvroValidatorException::class);
        self::expectExceptionMessage(json_encode(['test' => 'test']));
        self::assertNotSame($producerMessage, $encoder->encode($producerMessage));
    }
}

class RecordRegistry {
    public function fromSchema(string $schema): string
    {
        return $schema;
    }
}

class Validator {
    public function validate(): array
    {
        return [
            'test' => 'test',
        ];
    }
}

class AvroValidationException {

}
