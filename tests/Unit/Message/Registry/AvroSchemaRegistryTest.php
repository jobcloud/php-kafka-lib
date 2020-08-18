<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Message\Registry;

use \AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Kafka\Exception\AvroSchemaRegistryException;
use Jobcloud\Kafka\Message\Encoder\AvroEncoderInterface;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistry;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Registry\AvroSchemaRegistry
 */
class AvroSchemaRegistryTest extends TestCase
{
    public function testAddBodySchemaMappingForTopic()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addBodySchemaMappingForTopic('test', $schema);

        $reflectionProperty = new \ReflectionProperty($registry, 'schemaMapping');
        $reflectionProperty->setAccessible(true);

        $schemaMapping = $reflectionProperty->getValue($registry);

        self::assertArrayHasKey(AvroSchemaRegistryInterface::BODY_IDX, $schemaMapping);
        self::assertArrayHasKey('test', $schemaMapping[AvroSchemaRegistryInterface::BODY_IDX]);
        self::assertSame($schema, $schemaMapping[AvroSchemaRegistryInterface::BODY_IDX]['test']);
    }

    public function testAddKeySchemaMappingForTopic()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addKeySchemaMappingForTopic('test2', $schema);

        $reflectionProperty = new \ReflectionProperty($registry, 'schemaMapping');
        $reflectionProperty->setAccessible(true);

        $schemaMapping = $reflectionProperty->getValue($registry);

        self::assertArrayHasKey(AvroSchemaRegistryInterface::KEY_IDX, $schemaMapping);
        self::assertArrayHasKey('test2', $schemaMapping[AvroSchemaRegistryInterface::KEY_IDX]);
        self::assertSame($schema, $schemaMapping[AvroSchemaRegistryInterface::KEY_IDX]['test2']);
    }

    public function testHasBodySchemaMappingForTopic()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);
        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);

        $registry = new AvroSchemaRegistry($flixRegistry);
        $registry->addBodySchemaMappingForTopic('test', $schema);

        self::assertTrue($registry->hasBodySchemaForTopic('test'));
        self::assertFalse($registry->hasBodySchemaForTopic('test2'));
    }

    public function testHasKeySchemaMappingForTopic()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);
        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);

        $registry = new AvroSchemaRegistry($flixRegistry);
        $registry->addKeySchemaMappingForTopic('test', $schema);

        self::assertTrue($registry->hasKeySchemaForTopic('test'));
        self::assertFalse($registry->hasKeySchemaForTopic('test2'));
    }

    public function testGetBodySchemaForTopicWithNoMapping()
    {
        self::expectException(AvroSchemaRegistryException::class);
        self::expectExceptionMessage(
            sprintf(
                AvroSchemaRegistryException::SCHEMA_MAPPING_NOT_FOUND,
                'test',
                AvroSchemaRegistryInterface::BODY_IDX
            )
        );

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->getBodySchemaForTopic('test');
    }

    public function testGetBodySchemaForTopicWithMappingWithDefinition()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $schema->expects(self::once())->method('getDefinition')->willReturn($definition);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addBodySchemaMappingForTopic('test', $schema);

        self::assertSame($schema, $registry->getBodySchemaForTopic('test'));
    }

    public function testGetKeySchemaForTopicWithMappingWithDefinition()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $schema->expects(self::once())->method('getDefinition')->willReturn($definition);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addKeySchemaMappingForTopic('test2', $schema);

        self::assertSame($schema, $registry->getKeySchemaForTopic('test2'));
    }

    public function testGetBodySchemaForTopicWithMappingWithoutDefinitionLatest()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);
        $flixRegistry->expects(self::once())->method('latestVersion')->with('test-schema')->willReturn($definition);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $schema->expects(self::once())->method('getDefinition')->willReturn(null);
        $schema->expects(self::once())->method('getVersion')->willReturn(KafkaAvroSchemaInterface::LATEST_VERSION);
        $schema->expects(self::once())->method('getName')->willReturn('test-schema');
        $schema->expects(self::once())->method('setDefinition')->with($definition);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addBodySchemaMappingForTopic('test', $schema);

        $registry->getBodySchemaForTopic('test');
    }

    public function testGetBodySchemaForTopicWithMappingWithoutDefinitionVersion()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);
        $flixRegistry->expects(self::once())->method('schemaForSubjectAndVersion')->with('test-schema', 1)->willReturn($definition);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $schema->expects(self::once())->method('getDefinition')->willReturn(null);
        $schema->expects(self::exactly(2))->method('getVersion')->willReturn(1);
        $schema->expects(self::once())->method('getName')->willReturn('test-schema');
        $schema->expects(self::once())->method('setDefinition')->with($definition);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addBodySchemaMappingForTopic('test', $schema);

        $registry->getBodySchemaForTopic('test');
    }

    public function testGetTopicSchemaMapping()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        self::assertIsArray($registry->getTopicSchemaMapping());
    }
}
