<?php

/**
 * @created     28.06.2017
 */

namespace Tochka\Integration\Stomp;

use Stomp;
use StompException;
use Exception;
use Psr\Log\LogLevel;
use Tochka\Integration\Stomp\Exception\StompClientException;

/**
 * @author Sergey Ivanov(ivanov@tochka.com)
 */
class StompClient
{
    use Traits\Loggable;

    /**
     * @var Stomp
     */
    private $stomp;

    private $hosts;
    private $login;
    private $pw;

    /**
     * @var array
     */
    private $queues = [];

    /**
     * Примеры $connectionString:
     * - Use only one broker uri: tcp://localhost:61614
     * - use failover in given order: failover://(tcp://localhost:61614,ssl://localhost:61612).
     *
     * @param  string               $connectionString hosts url
     * @param  string               $login            login
     * @param  string               $pw               password
     * @throws StompClientException
     */
    public function __construct(string $connectionString, string $login, string $pw)
    {
        $hosts = [];

        $pattern = "|^(([a-zA-Z0-9]+)://)+\(*([a-zA-Z0-9\.:/i,-_]+)\)*$|i";
        if (preg_match($pattern, $connectionString, $matches)) {
            $scheme = $matches[2];
            $hostsPart = $matches[3];

            if ('failover' != $scheme) {
                $hosts[] = $hostsPart;
            } else {
                $urls = explode(',', $hostsPart);
                foreach ($urls as $url) {
                    $hosts[] = $url;
                }
            }
        }

        if (empty($hosts)) {
            throw new StompClientException("Bad Broker URL {$connectionString}. Check used scheme!");
        }

        $this->hosts = $hosts;
        $this->login = $login;
        $this->pw = $pw;
    }

    public function __destruct()
    {
        $this->hosts = null;
        $this->login = null;
        $this->pw = null;
        if ($this->stomp) {
            foreach ($this->queues as $queue) {
                $this->stomp->unsubscribe($queue, ['id' => $stomp->getSessionId()]);
            }
        }
        $this->stomp = null;
        $this->queues = [];
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
    public function send(
        string $destination,
        string $body,
        array $headers = [],
        string $transactionId = '') {

        if (strlen($transactionId) === 0) {
            $transactionId = uniqid();
        }

        $result = true;
        try {
            $stomp = $this->getStomp();
            $stomp->begin($transactionId);
            $stomp->send($destination, $body, $headers);
            $stomp->commit($transactionId);
        } catch (Exception $e) {
            $result = false;

            $this->getStomp()->abort($transactionId);

            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp transaction failed. Transaction id: ' . $transactionId, [
                'Message' => $e->getMessage(),
                'Code' => $e->getCode(),
                'File' => $e->getFile(),
                'Line' => $e->getLine()
            ]);
        }

        return $result;
    }

    /**
     * @return array|null
     */
    public function getNextFrame()
    {
        try {
            $stomp = $this->getStomp();
            if (!$stomp->hasFrame()) {
                return null;
            }

            return $stomp->readFrame();
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::getNextFrame failed', [
                'Message' => $ex->getMessage(),
                'Code' => $ex->getCode(),
                'File' => $ex->getFile(),
                'Line' => $ex->getLine()
            ]);

            throw $ex;
        }
    }

    /**
     * @param $frame
     *
     * @return bool
     */
    public function ack($frame)
    {
        $id = !empty($frame->headers['ack']) ? $frame->headers['ack'] : $frame;

        try {
            return $this->getStomp()->ack($id, ['id' => $id]);
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::ack failed', [
                'Message' => $ex->getMessage(),
                'Code' => $ex->getCode(),
                'File' => $ex->getFile(),
                'Line' => $ex->getLine(),
                'frame' => $frame
            ]);

            throw $ex;
        }
    }

    /**
     * @param $frame
     *
     * @return mixed
     */
    public function nack($frame)
    {
        $id = !empty($frame->headers['ack']) ? $frame->headers['ack'] : $frame;

        try {
            return $this->getStomp()->nack($id, ['id' => $id]);
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::nack failed', [
                'Message' => $ex->getMessage(),
                'Code' => $ex->getCode(),
                'File' => $ex->getFile(),
                'Line' => $ex->getLine(),
                'frame' => $frame
            ]);

            throw $ex;
        }
    }
    
    /**
     * @param array $queues Массив очередей, к которым нужно подписаться
     * @return void
     */
    public function subscribe($queues)
    {
        if (is_string($queues)) {
            $this->queues = [ $queues ];
        } else {
            $this->queues = $queues;
        }

        try {
            $stomp = $this->getStomp();
            foreach ($this->queues as $queue) {
                $stomp->subscribe($queue, ['id' => $stomp->getSessionId()]);
            }
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::subscribe failed', [
                'Message' => $ex->getMessage(),
                'Code' => $ex->getCode(),
                'File' => $ex->getFile(),
                'Line' => $ex->getLine(),
                'queues' => $this->queues
            ]);

            throw $ex;
        }
    }

    /**
     * Отписываемся от всех очередей
     *
     * @return void
     */
    public function unsubscribe()
    {
        if (count($this->queues) === 0) {
            return;
        }

        try {
            $stomp = $this->getStomp();
            foreach ($this->queues as $queue) {
                $stomp->unsubscribe($queue, ['id' => $stomp->getSessionId()]);
            }
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::unsubscribe failed', [
                'Message' => $ex->getMessage(),
                'Code' => $ex->getCode(),
                'File' => $ex->getFile(),
                'Line' => $ex->getLine(),
                'queues' => $this->queues
            ]);

            throw $ex;
        }
    }

    /**
     * Возвращает коннект первого доступного брокера
     * В случае необходимости устанавливает коннект
     *
     * @return null|Stomp
     */
    private function getStomp()
    {
        if (is_null($this->stomp)) {
            $this->stomp = $this->initStomp();
        }

        if (!empty($this->stomp->error())) {
            $this->putInLog(LogLevel::INFO, 'Stomp::getStomp error', [
                'error' => $this->stomp->error(),
            ]);

            $this->stomp = $this->initStomp();
        }

        return $this->stomp;
    }

    /**
     * Возвращает коннект первого доступного брокера.
     *
     * @return Stomp
     */
    private function initStomp()
    {
        $errors = [];
        $i = 0;
        foreach ($this->hosts as $host) {
            $i++;
            try {
                $connect = $this->connect($host, $this->login, $this->pw);
                $this->putInLog(LogLevel::INFO, 'Stomp::initStomp connected', [
                    'host' => $host,
                ]);

                return $connect;
            } catch (Exception $ex) {
                $errors[] = $ex->getMessage();
                if ($i === count($this->hosts)) {
                    throw new StompClientException("StompClient cannot connect to: " . implode(', ',$this->hosts) . '.Errors: ' . implode(', ',$errors));
                }
            }
        }
    }

    /**
     * Подключение к брокеру по ссылке.
     *
     * @param  string     $url
     * @param  string     $login
     * @param  string     $pw
     * @return Stomp|null
     */
    private function connect($url, $login, $pw)
    {
        try {
            return new Stomp($url, $login, $pw, ['accept-version' => '1.2', 'RECEIPT' => true]);
        } catch (StompException $e) {
            throw $e;
        }
    }
}
