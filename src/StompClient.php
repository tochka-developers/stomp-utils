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
 *
 * @property Stomp  stomp
 * @property string login
 * @property array  hosts
 * @property array  queues
 * @property string pw
 * @property string vhost
 */
class StompClient
{
    use Traits\Loggable;

    /**
     * @var Stomp
     */
    private $stomp;

    /** @var array $hosts */
    private $hosts;

    /** @var string $login */
    private $login;

    /** @var string $pw */
    private $pw;

    /** @var string $vhost */
    private $vhost;

    /**
     * @var array
     */
    private $queues = [];

    /**
     * Примеры $connectionString:
     * - Use only one broker uri: tcp://localhost:61614
     * - use failover in given order: failover://(tcp://localhost:61614,ssl://localhost:61612).
     *
     * @param string      $connectionString hosts url
     * @param string      $login login
     * @param string      $pw password
     * @param string|null $vhost
     *
     * @throws StompClientException
     */
    public function __construct(
        string $connectionString,
        string $login,
        string $pw,
        string $vhost = null
    )
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
        $this->vhost = $vhost;
    }

    public function __destruct()
    {
        $this->hosts = null;
        $this->login = null;
        $this->pw = null;
        $this->stomp = null;
        $this->queues = [];
        $this->vhost = null;
    }

    /**
     * Опубликовать сообщение в очереди
     *
     * @param string $destination Очередь, куда класть сообщение
     * @param string $body Тело сообщения
     * @param array  $headers
     *
     * @return boolean
     */
    public function send(
        string $destination,
        string $body,
        array $headers = [])
    {

        $result = true;
        try {
            $result = $this->getStomp()->send($destination, $body, $headers);
        } catch (Exception $e) {
            $result = false;

            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::send failed.', [
                'Message' => $e->getMessage(),
                'headers' => implode(', ', $headers),
            ]);
        }

        return $result;
    }

    /**
     * @return \StompFrame|null
     * @throws Exception
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
                'Code'    => $ex->getCode(),
                'File'    => $ex->getFile(),
                'Line'    => $ex->getLine(),
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
            if (!$this->getStomp()->ack($id, ['id' => $id])) {
                throw new Exception('Frame with id: ' . $id . ' does not acked');
            }

            return true;
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::ack failed', [
                'Message' => $ex->getMessage(),
                'Code'    => $ex->getCode(),
                'File'    => $ex->getFile(),
                'Line'    => $ex->getLine(),
                'frame'   => $frame,
            ]);

            return false;
        }
    }

    /**
     * @param $frame
     *
     * @return bool
     */
    public function nack($frame)
    {
        $id = !empty($frame->headers['ack']) ? $frame->headers['ack'] : $frame;

        try {
            if (!$this->getStomp()->nack($id, ['id' => $id])) {
                throw new Exception('Frame with id: ' . $id . ' does not nacked');
            }

            return true;
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::nack failed', [
                'Message' => $ex->getMessage(),
                'Code'    => $ex->getCode(),
                'File'    => $ex->getFile(),
                'Line'    => $ex->getLine(),
                'frame'   => $frame,
            ]);

            return false;
        }
    }

    /**
     * @param array $queues Массив очередей, к которым нужно подписаться
     *
     * @return void
     * @throws Exception
     */
    public function subscribe($queues)
    {
        if (is_string($queues)) {
            $this->queues = [$queues];
        } else {
            $this->queues = $queues;
        }

        try {
            foreach ($this->queues as $queue) {
                $stomp = $this->getStomp();
                if (!$stomp->subscribe($queue, ['id' => $stomp->getSessionId()])) {
                    throw new Exception('Queue: ' . $queue);
                }
            }
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::subscribe failed', [
                'Message' => $ex->getMessage(),
                'Code'    => $ex->getCode(),
                'File'    => $ex->getFile(),
                'Line'    => $ex->getLine(),
                'queues'  => $this->queues,
            ]);

            throw $ex;
        }
    }

    /**
     * Отписываемся от всех очередей
     *
     * @return void
     * @throws Exception
     */
    public function unsubscribe()
    {
        if (count($this->queues) === 0) {
            return;
        }

        try {
            foreach ($this->queues as $queue) {
                $stomp = $this->getStomp();
                if (!$stomp->unsubscribe($queue, ['id' => $stomp->getSessionId()])) {
                    throw new Exception('Queue: ' . $queue);
                }
            }
        } catch (Exception $ex) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'Stomp::unsubscribe failed', [
                'Message' => $ex->getMessage(),
                'Code'    => $ex->getCode(),
                'File'    => $ex->getFile(),
                'Line'    => $ex->getLine(),
                'queues'  => $this->queues,
            ]);

            throw $ex;
        }
    }

    /**
     * Возвращает коннект первого доступного брокера
     * В случае необходимости устанавливает коннект
     *
     * @return null|Stomp
     * @throws StompClientException
     */
    private function getStomp()
    {
        if (is_null($this->stomp)) {
            $this->stomp = $this->initStomp();
        }

        if (!empty($this->stomp->error())) {
            $this->putInLog(LogLevel::WARNING, 'Stomp::getStomp error', [
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
     * @throws StompClientException
     */
    private function initStomp()
    {
        $errors = [];
        $i = 0;
        /** @var string $host */
        foreach ($this->hosts as $host) {
            $i++;
            try {
                $connect = $this->connect($host, $this->login, $this->pw, $this->vhost);
                $this->putInLog(LogLevel::WARNING, 'Stomp::initStomp connected', [
                    'host' => $host,
                ]);

                return $connect;
            } catch (Exception $ex) {
                $errors[] = $ex->getMessage();
                if ($i === count($this->hosts)) {
                    throw new StompClientException("Cannot connect to: " . implode(', ', $this->hosts) . '.Errors: ' . implode(', ', $errors));
                }
            }
        }
    }

    /**
     * Подключение к брокеру по ссылке.
     *
     * @param string      $url
     * @param string      $login
     * @param string      $pw
     * @param string|null $vhost
     *
     * @return Stomp|null
     * @throws Exception
     */
    private function connect(
        string $url,
        string $login,
        string $pw,
        string $vhost
    )
    {
        $headers = [
            'accept-version' => '1.2',
            'RECEIPT'        => true,
        ];

        if ($vhost) {
            $headers['host'] = $vhost;
        }

        try {
            return new Stomp($url, $login, $pw, $headers);
        } catch (StompException $e) {
            throw $e;
        }
    }
}
