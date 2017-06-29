<?php

/**
 * @created     29.06.2017
 */
namespace Tochka\Integration\Stomp;

use \StompFrame;

/**
 * Описание FrameMapper
 *
 * @author Sergey Ivanov(ivanov@tochka.com)
 */
class FrameMapper
{
    /**
     * @var StompFrame
     */
    private $frame;

    /**
     * @param StompFrame $frame
     */
    public function __construct(StompFrame $frame)
    {
        $this->frame = $frame;
    }

    /**
     * Получить фрейм
     *
     * @return StompFrame
     */
    public function getFrame(): StompFrame
    {
        return $this->frame;
    }

    /**
     * Получить все заголовки фрейма
     *
     * @return array
     */
    public function getHeaders(): array
    {
        return $this->frame->headers;
    }

    /**
     * Получить значение конкретного заголовка фрейма
     *
     * @param string $name Название заголовка, который нужно получить
     * @return string|null
     */
    public function getHeader($name)
    {
        return $this->frame->headers[$name] || null;
    }

    /**
     * Получить тело фрейма
     *
     * @return string
     */
    public function getBody(): string
    {
        return $this->frame->body;
    }
}
