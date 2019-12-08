<?php


namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Message;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;

/**
 * @ \Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback
 */
class KafkaProducerDeliveryReportCallbackTest extends TestCase
{
    public function getProducerMock()
    {
        return $this->getMockBuilder(RdKafkaProducer::class)
            ->disableOriginalConstructor()
            ->getMock();
    }

    public function testInvokeDefault()
    {
        self::expectException(KafkaProducerException::class);

        $message = new Message();
        $message->err = -1;

        call_user_func(new KafkaProducerDeliveryReportCallback(), $this->getProducerMock(), $message);
    }

    public function testInvokeTimeout()
    {
        self::expectException(KafkaProducerException::class);

        $message = new Message();
        $message->err = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;

        call_user_func(new KafkaProducerDeliveryReportCallback(), $this->getProducerMock(), $message);
    }

    public function testInvokeNoError()
    {
        $message = new Message();
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;

        $result = call_user_func(new KafkaProducerDeliveryReportCallback(), $this->getProducerMock(), $message);

        self::assertNull($result);
    }
}
