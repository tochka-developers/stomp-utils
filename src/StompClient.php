<?php

namespace Tochka\Integration\Stomp;

use \Stomp;
use \StompException;
use Tochka\Integration\Stomp\Exception\StompClientException;

/**
 * Class StompClient
 * @package Tochka\Integration\Stomp
 */
class StompClient
{
    /**
     * @var Stomp
     */
    protected $stomp;

    protected $hosts;
    protected $login;
    protected $pw;

    protected $errors = [];

    /**
     * Примеры $connectionString:
     * - Use only one broker uri: tcp://localhost:61614
     * - use failover in given order: failover://(tcp://localhost:61614,ssl://localhost:61612)
     *
     * @param string $connectionString hosts url
     * @param string $login login
     * @param string $pw password
     * @throws StompClientException
     */
    public function __construct(string $connectionString, string $login, string $pw)
    {
        $hosts = [];

        $pattern = "|^(([a-zA-Z0-9]+)://)+\(*([a-zA-Z0-9\.:/i,-_]+)\)*$|i";
        if (preg_match($pattern, $connectionString, $matches)) {

            list(, , $scheme, $hostsPart) = $matches;

            if ($scheme !== 'failover') {
                $hosts[] = $hostsPart;
            } else {
                foreach (explode(',', $hostsPart) as $url) {
                    $hosts[] = $url;
                }
            }
        }

        if (empty($hosts)) {
            throw new StompClientException('Bad Broker URL ' . $connectionString . 'Check used scheme!');
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
        $this->stomp = null;
    }

    /**
     * @return Stomp
     * @throws StompClientException
     */
    public function getConnection(): \Stomp
    {
        if (null === $this->stomp) {
            $this->newConnection();
        }

        return $this->stomp;
    }

    /**
     * Устанавливает новое соединение
     *
     * @throws StompClientException
     * @return Stomp
     */
    public function newConnection()
    {
        $this->errors = [];
        $stomp = $this->initStomp();
        if (!($stomp instanceof Stomp)) {
            throw new StompClientException("Couldn't connect to Brocker by provided hosts: " . implode('; ', $this->errors));
        }

        $this->stomp = $stomp;

        return $this->stomp;
    }

    /**
     * Возвращает коннект первого доступного брокера
     *
     * @return null|Stomp
     */
    private function initStomp()
    {
        foreach ($this->hosts as $host) {
            if ($connection = $this->connect($host, $this->login, $this->pw)) {
                return $connection;
            }
        }

        return null;
    }

    /**
     * Подключение к брокеру по ссылке
     *
     * @param string $url
     * @param string $login
     * @param string $pw
     * @return Stomp|null
     */
    private function connect($url, $login, $pw)
    {
        try {
            return new Stomp($url, $login, $pw, ['accept-version' => '1.2']);
        } catch (StompException $e) {
            $this->errors[] = $url . ': ' . $e->getMessage();
        }

        return null;
    }
}