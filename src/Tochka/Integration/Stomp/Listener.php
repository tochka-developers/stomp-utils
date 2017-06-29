<?php

/**
 * @created     28.06.2017
 */
namespace Tochka\Integration\Stomp;

use Psr\Log\LogLevel;

class Listener
{
    use Traits\Loggable;

    /**
     * @var int
     */
    public $maxIterations = 200;

    /**
     * @var string
     */
    private $queue;

    /**
     * @var StompClient
     */
    private $client;

    /**
     * Listener constructor
     *
     * @param string $queue
     * @param StompClient $client
     */
    public function __construct(string $queue, StompClient $client)
    {
        $this->queue = $queue;
        $this->client = $client;
    }

    /**
     * @return void
     */
    public function pull()
    {
        $i = 0;

        try {
            while (true) {
                // reconnect
                if (empty($i) || $i === $this->maxIterations) {
                    unset($broker);
                    $broker = $this->client->newConnection();
                    if (empty($broker)) {
                        continue;
                    }

                    $broker->subscribe($this->queue, ['id' => $broker->getSessionId()]);
                    $i = 0;
                }

                $i++;

                if (!$broker->hasFrame()) {
                    continue;
                }
                if ($frame = $broker->readFrame()) {
                    $mapper = $this->generateFrameMapper($frame);
                    $handler = $this->generateHandler($mapper);
                    if ($handler->handle()) {
                        $broker->ack($frame, ['id' => $frame->headers['ack']]);
                    } else {
                        $broker->nack($frame, ['id' => $frame->headers['ack']]);
                    }
                }
            }
        } catch (\StompException $e) {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::ERROR, 'StompException: ' . $e->getMessage(), [
                'Message: ' . $e->getMessage(),
                'Code: ' . $e->getCode(),
                'File: ' . $e->getFile(),
                'Line: ' . $e->getLine()
            ]);
        } finally {
            // Логирование в случае, если установлен логгер
            $this->putInLog(LogLevel::INFO, 'Queue listener crash', [$this->queue]);
        }
    }

    protected function generateFrameMapper($frame): FrameMapper
    {
        return new FrameMapper($frame);
    }

    protected function generateHandler(FrameMapper $mapper): BaseWorker
    {
        return new BaseWorker($mapper);
    }
}
