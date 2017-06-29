<?php

/**
 * @created     29.06.2017
 */
namespace Tochka\Integration\Stomp\Traits;

use Psr\Log\LoggerInterface;

/**
 *
 * @author Sergey Ivanov(ivanov@tochka.com)
 */
trait Loggable
{
    /**
     * @var LoggerInterface
     */
    protected $logger;

    public function setLogger(LoggerInterface $logger = null)
    {
        $this->logger = $logger;
    }

    /**
     * Logs with an arbitrary level
     *
     * @param mixed $level
     * @param string $message
     * @param array $context
     * @return null
     */
    protected function putInLog($level, $message, array $context = [])
    {
        if ($this->logger) {
            $this->logger->log($level, $message, $context);
        }
    }
}
