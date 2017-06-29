<?php

/**
 * @created     28.06.2017
 */
namespace Tochka\Integration\Stomp;

class BaseJob
{
    /**
     * @var FrameMapper
     */
    protected $mapper;

    /**
     * @param FrameMapper $mapper
     */
    public function __construct(FrameMapper $mapper)
    {
        $this->mapper = $mapper;
    }

    /**
     * @return bool
     */
    public function handle()
    {
        return true;
    }

}
