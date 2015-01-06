<?php
/**
 * RedisClient.php
 *
 * @copyright   Copyright (c) 2014 sonicmoov Co.,Ltd.
 * @version     $Id$
 *
 */


namespace Iwai\React\Resque;

use Clue\React\Redis;

class RedisClient {


    /**
     * Redis namespace
     * @var string
     */
    private static $defaultNamespace = 'resque:';

    /**
     * @var array List of all commands in Redis that supply a key as their
     *	first argument. Used to prefix keys with the Resque namespace.
     */
    private $keyCommands = array(
        'exists',
        'del',
        'type',
        'keys',
        'expire',
        'ttl',
        'move',
        'set',
        'get',
        'getset',
        'setnx',
        'incr',
        'incrby',
        'decr',
        'decrby',
        'rpush',
        'lpush',
        'llen',
        'lrange',
        'ltrim',
        'lindex',
        'lset',
        'lrem',
        'lpop',
        'rpop',
        'sadd',
        'srem',
        'spop',
        'scard',
        'sismember',
        'smembers',
        'srandmember',
        'zadd',
        'zrem',
        'zrange',
        'zrevrange',
        'zrangebyscore',
        'zcard',
        'zscore',
        'zremrangebyscore',
        'sort'
    );
    // sinterstore
    // sunion
    // sunionstore
    // sdiff
    // sdiffstore
    // sinter
    // smove
    // rename
    // rpoplpush
    // mget
    // msetnx
    // mset
    // renamenx

    protected $target;
    protected $loop;
    protected $client;


    function __construct($target, $loop)
    {
        $this->target = $target;
        $this->loop   = $loop;

        $this->client = (new Redis\Factory($loop))->createClient($target);
    }

    /**
     * Set Redis namespace (prefix) default: resque
     * @param string $namespace
     */
    public function prefix($namespace)
    {
        if (strpos($namespace, ':') === false) {
            $namespace .= ':';
        }
        self::$defaultNamespace = $namespace;
    }

    public function getPrefix()
    {
        return self::$defaultNamespace;
    }

    function establishConnection() {
        $this->client = (new Redis\Factory($this->loop))->createClient($this->target);
    }

    /**
     * Magic method to handle all function requests and prefix key based
     * operations with the {self::$defaultNamespace} key prefix.
     *
     * @param string $name The name of the method called.
     * @param array  $args Array of supplied arguments to the method.
     *
     * @return \React\Promise\PromiseInterface mixed Return value from Resident::call() based on the command.
     */
    public function __call($name, $args)
    {
        $args = func_get_args();
        if(in_array($name, $this->keyCommands)) {
            $args[1][0] = self::$defaultNamespace . $args[1][0];
        }
        return $this->client->then(function (Redis\Client $client) use ($name, $args) {
            return call_user_func_array([$client, $name], $args[1]);
        }, function (\Exception $e) {
            // TODO: throw はまずい
            throw $e;
        });
    }

}