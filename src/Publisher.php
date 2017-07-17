<?php

/**
 * @created     28.06.2017
 */
namespace Tochka\Integration\Stomp;

use Stomp;
use Psr\Log\LogLevel;

/**
 *
 * @author Sergey Ivanov(ivanov@tochka.com)
 */
class Publisher
{
    use Traits\Loggable;

    /**
     * @var Stomp
     */
    protected $stomp;

    /**
     * Publisher constructor
     * @param Stomp $stomp
     */
    public function __construct(Stomp $stomp)
    {
        $this->stomp = $stomp;
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
            $this->stomp->begin($transactionId);
            $this->stomp->send($destination, $body, $headers);
            $this->stomp->commit($transactionId);
        } catch (\Exception $e) {
            $result = false;

            $this->stomp->abort($transactionId);

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
