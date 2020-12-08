<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Conf;

use Jobcloud\Kafka\Exception\KafkaSllCertsCreationException;

class KafkaSslCertsOptionalConfiguration
{
    private const SSL_CA_FILE_NAME = 'kafka_ca.pem';
    private const SSL_CERTIFICATE_FILE_NAME = 'kafka_certificate.cert';
    private const SSL_KEY_FILE_NAME = 'kafka_key.key';

    private const SSL_CERT_FOLDER_PERMISSIONS = 0700;
    private const SSL_CERT_FILE_PERMISSIONS = 0600;

    /**
     * @var mixed[]
     */
    private $config;

    /**
     * @param mixed[] $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;
    }

    public function isProvided(): bool
    {
        return (isset(
            $this->config[KafkaConfiguration::SSL_CA_CONF_KEY],
            $this->config[KafkaConfiguration::SSL_CERTIFICATE_CONF_KEY],
            $this->config[KafkaConfiguration::SSL_KEY_CONF_KEY],
            $this->config[KafkaConfiguration::SSL_FOLDER_CONF_KEY]
        ));
    }

    /**
     * @return mixed[]
     */
    public function createSslCertsAndUpdateConfig(): array
    {
        $config = $this->config;

        if (!is_dir($config[KafkaConfiguration::SSL_FOLDER_CONF_KEY])) {
            mkdir($config[KafkaConfiguration::SSL_FOLDER_CONF_KEY], self::SSL_CERT_FOLDER_PERMISSIONS, true);
        }

        $config['ssl.ca.location'] = $config[KafkaConfiguration::SSL_FOLDER_CONF_KEY] . self::SSL_CA_FILE_NAME;
        $config['ssl.certificate.location'] =
            $config[KafkaConfiguration::SSL_FOLDER_CONF_KEY] . self::SSL_CERTIFICATE_FILE_NAME;
        $config['ssl.key.location'] = $config[KafkaConfiguration::SSL_FOLDER_CONF_KEY] . self::SSL_KEY_FILE_NAME;

        foreach (
            [
                $config['ssl.ca.location'] => $config[KafkaConfiguration::SSL_CA_CONF_KEY],
                $config['ssl.certificate.location'] => $config[KafkaConfiguration::SSL_CERTIFICATE_CONF_KEY],
                $config['ssl.key.location'] => $config[KafkaConfiguration::SSL_KEY_CONF_KEY],
            ] as $filename => $secret
        ) {
            if (true === file_exists($filename) && $secret === file_get_contents($filename)) {
                continue;
            }

            if (false === file_put_contents($filename, $secret)) {
                throw new KafkaSllCertsCreationException(sprintf(
                    '%s %s',
                    KafkaSllCertsCreationException::UNABLE_TO_CREATE_KAFKA_CERT,
                    $filename
                ));
            }

            chmod($filename, self::SSL_CERT_FILE_PERMISSIONS);
        }

        return $config;
    }
}
