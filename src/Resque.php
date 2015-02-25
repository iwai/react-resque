<?php
/**
 * Resque.php
 *
 * @copyright   Copyright (c) 2014 sonicmoov Co.,Ltd.
 * @version     $Id$
 *
 */


namespace Iwai\React;

use Iwai\React\Resque\RedisClient;
use React\EventLoop\LoopInterface;
use Clue\React\Redis;

class Resque extends \Resque {

    /**
     * @var LoopInterface
     */
    protected static $loop;

    /**
     * @param LoopInterface $loop
     */
    public static function setEventLoop($loop)
    {
        self::$loop = $loop;
    }

    /**
     * @return LoopInterface $loop
     */
    public static function getEventLoop()
    {
        return self::$loop;
    }

    public static function redis()
    {
        // Detect when the PID of the current process has changed (from a fork, etc)
        // and force a reconnect to redis.
        if (!self::$pid) {
            self::$redis = null;
            self::$pid   = getmypid();
        }

        if(!is_null(self::$redis)) {
            return self::$redis;
        }

        $server = self::$redisServer;
        if (empty($server)) {
            $server = 'localhost:6379';
        }

        if(is_array($server)) {
            die('Unsupported multiple redis server.');
        }
        else {
            $client = new RedisClient($server, self::$loop);
            self::$redis = $client;
        }
        self::$redis->prefix(self::$namespace);
        self::$redis->select(self::$redisDatabase);

        return self::$redis;
    }

    /**
     * Pop an item off the end of the specified queue, decode it and
     * return it.
     *
     * @param array|string $queues
     * @param int $interval
     *
     * @return array Decoded item from the queue.
     */
    public static function bpop($queues, $interval = 5)
    {
        $args = [];
        foreach ($queues as $queue) {
            $args[] = self::redis()->getPrefix() . 'queue:' . $queue;
        }
        $args[] = $interval;

        return call_user_func_array([self::redis(), 'blpop'], $args);
    }

    public static function queues()
    {
        return self::redis()->smembers('queues');
    }

} 