<?php
/**
 * Worker.php
 *
 * @copyright   Copyright (c) 2014 sonicmoov Co.,Ltd.
 * @version     $Id$
 *
 */


namespace Iwai\React\Resque;


//use Clue\Redis\Protocol\Model\ModelInterface;
//use Evenement\EventEmitter;
use Iwai\React\Resque;
use Iwai\React\Resque\Job;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use React\Promise;
use MKraemer\ReactPCNTL\PCNTL;

//use Resque_Event;
//use Resque_Job_DirtyExitException;
use Resque_Worker;

class Worker extends \Resque_Worker {

    public $processing = 0;
    public $waitShutdown = false;

    protected $options;

    protected $loop;

    /**
     * Return all workers known to Resque as instantiated instances.
     * @return PromiseInterface
     */
    public static function all()
    {
        return Resque::redis()->smembers('workers')->then(function ($response) {

            return \React\Promise\map($response, function ($workerId) {

                return Resque::redis()->sismember(
                    'workers', $workerId
                )->then(function ($exists) use ($workerId) {
                    if (!$exists)
                        return;

                    list($hostname, $pid, $queues) = explode(':', $workerId, 3);
                    $queues = explode(',', $queues);
                    $worker = new self($queues, \Iwai\React\Resque::getEventLoop());
                    $worker->setId($workerId);
                    return $worker;
                });

            });

        });
    }

    /**
     * @param array|string  $options
     * @param LoopInterface $loop
     *
     * @internal param array|string $queues
     */
    public function __construct($options, LoopInterface $loop = null)
    {
        $this->options = $options;
        $this->loop    = $loop;

        if ($loop !== null)
            Resque::setEventLoop($loop);

        parent::__construct($options['queue']);
    }

    protected $startTime;
    protected $startTimeP;

    public function work($interval = 5)
    {
        if ($this->shutdown) {
            if (!$this->waitShutdown) {
                $this->retry($this->loop, function () {
                    if ($this->processing === 0)
                        return \React\Promise\resolve(null);
                    sleep(1);
                    return \React\Promise\reject();
                })->then(function () {
                    return $this->unregisterWorker();
                })->then(function ($responses) {
                    return Resque::redis()->close();
                })->then(function ($response = null) {
                    $this->loop->stop();
                });
                $this->waitShutdown = true;
            }
        }

//        if ($this->startTime)
//            error_log(sprintf('time: %.5f', microtime(true) - $this->startTime));
        $this->startTime = microtime(true);
        $this->startTimeP = microtime(true);

        $this->reserve()->then(
            function ($response) use ($interval) {
                //error_log(sprintf('reserve time: %.6f', microtime(true) - $this->startTimeP));
                $this->startTimeP = microtime(true);

                if (!$response)
                    return;

                list($name, $payload) = $response;

                $names     = explode(':', $name);
                $queueName = array_pop($names);

                $job = new Job($queueName, json_decode($payload, true));
                $job->worker = $this;

                $job->perform();

                //error_log(sprintf('perform time: %.6f', microtime(true) - $this->startTimeP));
            },
            function (\Exception $e) {
                throw $e;
            }
        )->then(
            function () use ($interval) {
                $this->work($interval);
            },
            function (\Exception $e) use ($interval) {
                error_log(sprintf('%s:%s in %s at %d',
                    get_class($e), $e->getMessage(), __FILE__, __LINE__));
                error_log($e);

                $this->shutdown();
                $this->work($interval);
            }
        );

    }

    public function run($interval = 5)
    {
        $this->startup()->then(function () use ($interval) {
            pcntl_alarm(0);
            $this->work($interval);
        });
        $this->loop->run();
    }

    /**
     * Attempt to find a job from the top of one of the queues for this worker.
     *
     * @return PromiseInterface
     */
    public function reserve()
    {
        if ($this->waitShutdown) {
            sleep($this->options['interval']);
            return \React\Promise\resolve();
        }

        $queues = $this->queues();

        if ($queues instanceof PromiseInterface) {
            if ($this->paused) {
                return $queues->then(function () {
                    sleep($this->options['interval']);
                });
            } else {
                return $queues->then(function ($response) {
                    if (empty($response)) {
                        sleep($this->options['interval']);
                        return \React\Promise\resolve(null);
                    }

                    return Resque::bpop(
                        $response, $this->options['interval']
                    );
                });
            }
        } else {
            return Resque::bpop($queues, $this->options['interval']);
        }
    }

    /**
     * Process a single job.
     *
     * @param \Resque_Job $job The job to be processed.
     */
    public function perform(\Resque_Job $job)
    {
        /** @var Job $job */
        $job->worker = $this;

        return $job->perform();
    }

    public function queues($fetch = true)
    {
        if (!in_array('*', $this->queues) || $fetch == false) {
            return $this->queues;
        }

        return Resque::queues();
    }

    /**
     * @return PromiseInterface
     */
    protected function startup()
    {
        pcntl_alarm(30);
        $this->registerSigHandlers();
        $this->pruneDeadWorkers();

        return $this->registerWorker();
    }

    /**
     * Register this worker in Redis.
     *
     * @return PromiseInterface
     */
    public function registerWorker()
    {
        return Resque::redis()->sadd('workers', (string)$this)->then(function () {
            return Resque::redis()->set(
                'worker:' . (string)$this . ':started',
                strftime('%a %b %d %H:%M:%S %Z %Y')
            );
        });
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     *
     * @return PromiseInterface
     */
    public function unregisterWorker()
    {
//        if (is_object($this->currentJob)) {
//            $this->currentJob->fail(new \Resque_Job_DirtyExitException());
//        }

        $id = (string)$this;

        return \React\Promise\all([
            Resque::redis()->srem('workers', $id),
            Resque::redis()->del('worker:' . $id),
            Resque::redis()->del('worker:' . $id . ':started'),
            \Resque_Stat::clear('processed:' . $id),
            \Resque_Stat::clear('failed:' . $id),
            Resque::redis()->hdel('workerLogger', $id),
        ]);
    }

    public function pruneDeadWorkers()
    {
        $workerPids = $this->workerPids();

        self::all()->then(function ($workers) use ($workerPids) {
            foreach ($workers as $worker) {
                if (!($worker instanceof Worker))
                    continue;

                list($host, $pid, $queues) = explode(':', (string)$worker, 3);
                if ($host != $this->hostname || in_array($pid, $workerPids) || $pid == getmypid()) {
                    continue;
                }
                $worker->unregisterWorker();
            }
        });
    }

    protected function registerSigHandlers()
    {
        if (!function_exists('pcntl_signal')) {
            $this->log(array(
                    'message' => 'Signals handling is unsupported',
                    'data' => array('type' => 'signal')
                ), self::LOG_TYPE_WARNING
            );
            return;
        }

        $pcntl = new PCNTL($this->loop);

        foreach ([ SIGTERM, SIGINT, SIGQUIT, SIGUSR1] as $signal) {
            $pcntl->on($signal, function () {
                $this->shutdown();
            });
        }

        $pcntl->on(SIGUSR2, [ $this, 'pauseProcessing' ]);
        $pcntl->on(SIGCONT, [ $this, 'unPauseProcessing' ]);
        $pcntl->on(SIGPIPE, [ $this, 'reestablishRedisConnection' ]);

        //declare(ticks = 1);
        $this->log(array('message' => 'Registered signals', 'data' => array('type' => 'signal')), self::LOG_TYPE_DEBUG);
    }

    /**
     * @param LoopInterface $loop
     * @param \Closure      $callback
     * @param int           $interval
     * @param Deferred      $deferred
     *
     * @return PromiseInterface
     */
    private function retry($loop, $callback, $interval = 3, $deferred = null)
    {
        $deferred = $deferred ?: new \React\Promise\Deferred();

        /** @var PromiseInterface $promise */
        $promise = $callback();

        $promise->then(
            function ($response) use ($deferred) {
                $deferred->resolve($response);
            },
            function (\Exception $e = null) use ($loop, $callback, $interval, $deferred) {
                if ($e !== null)
                    echo sprintf('%s: %s', get_class($e), $e->getMessage()) . PHP_EOL;

                $loop->addTimer($interval,
                    function ($timer) use ($loop, $callback, $interval, $deferred) {
                        $this->retry($loop, $callback, $interval, $deferred);
                    }
                );
            }
        );

        return $deferred->promise();
    }

}
