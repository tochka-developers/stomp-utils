<?php

/**
 * @created     28.06.2017
 */
namespace Tochka\Integration\Stomp;

use Psr\Log\LogLevel;

/**
 *
 * @author Sergey Ivanov(ivanov@tochka.com)
 */
class Publisher
{
    use Traits\Loggable;

    /**
     * @var StompClient
     */
    protected $client;

    /**
     * Publisher constructor
     * @param StompClient $client
     */
    public function __construct(StompClient $client)
    {
        $this->client = $client;
    }

    /**
     * Опубликовать сообщение в очереди
     *
     * @param string $destination Очередь, куда класть сообщение
     * @param string $body Тело сообщения
     * @param array $headers
     * @param string $transactionId ID транзакции
     * @return boolean
     */
    public function send(string $destination, string $body, array $headers = [], string $transactionId = '')
    {
        if (strlen($transactionId) === 0) {
            $transactionId = uniqid();
        }

        $result = true;
        try {
            $stomp = $this->client->getConnection();
            $stomp->begin($transactionId);
            $stomp->send($destination, $body, $headers);
            $stomp->commit($transactionId);
        } catch (\Exception $e) {
            $result = false;

            if (isset($stomp)) {
                $stomp->abort($transactionId);
            }

            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp transaction failed. Transaction id: ' . $transactionId, [
                'Message: ' . $e->getMessage(),
                'Code: ' . $e->getCode(),
                'File: ' . $e->getFile(),
                'Line: ' . $e->getLine()
            ]);
        }

        return $result;
    }
}