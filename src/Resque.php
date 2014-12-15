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
        $pid = getmypid();
        if (self::$pid !== $pid) {
            self::$redis = null;
            self::$pid   = $pid;
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
//            if (strpos($server, 'unix:') === false) {
//                list($host, $port) = explode(':', $server);
//            }
//            else {
//                $host = $server;
//                $port = null;
//            }

            $client = new RedisClient($server, self::$loop);
            $client->prefix(self::$namespace);
            self::$redis = $client;
        }
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