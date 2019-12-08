<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Registry;

use \AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistry;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistry
 */
class AvroSchemaRegistryTest extends TestCase
{
    public function testAddSchemaMappingForTopic()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addSchemaMappingForTopic('test', $schema);

        $reflectionProperty = new \ReflectionProperty($registry, 'schemaMapping');
        $reflectionProperty->setAccessible(true);

        $schemaMapping = $reflectionProperty->getValue($registry);

        self::assertArrayHasKey('test', $schemaMapping);
        self::assertSame($schema, $schemaMapping['test']);
    }

    public function testGetSchemaForTopicWithNoMapping()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        self::assertNull($registry->getSchemaForTopic('test'));
    }

    public function testGetSchemaForTopicWithMappingWithDefinition()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $schema->expects(self::once())->method('getDefinition')->willReturn($definition);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addSchemaMappingForTopic('test', $schema);

        self::assertSame($schema, $registry->getSchemaForTopic('test'));
    }

    public function testGetSchemaForTopicWithMappingWithoutDefinitionLatest()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $flixRegistry = $this->getMockForAbstractClass(Registry::class);
        $flixRegistry->expects(self::once())->method('latestVersion')->with('test-schema')->willReturn($definition);

        $schema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $schema->expects(self::once())->method('getDefinition')->willReturn(null);
        $schema->expects(self::once())->method('getVersion')->willReturn(null);
        $schema->expects(self::once())->method('getName')->willReturn('test-schema');
        $schema->expects(self::once())->method('setDefinition')->with($definition);

        $registry = new AvroSchemaRegistry($flixRegistry);

        $registry->addSchemaMappingForTopic('test', $schema);

        $registry->getSchemaForTopic('test');
    }

    public function testGetSchemaForTopicWithMappingWithoutDefinitionVersion()
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

        $registry->addSchemaMappingForTopic('test', $schema);

        $registry->getSchemaForTopic('test');
    }

    public function testGetTopicSchemaMapping()
    {
        $flixRegistry = $this->getMockForAbstractClass(Registry::class);

        $registry = new AvroSchemaRegistry($flixRegistry);

        self::assertIsArray($registry->getTopicSchemaMapping());
    }
}
