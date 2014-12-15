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

                return Resque::redis()->sismember('workers', $workerId)->then(function ($exists) use ($workerId) {
                    if (!$exists)
                        return;

                    list($hostname, $pid, $queues) = explode(':', $workerId, 3);
                    $queues = explode(',', $queues);
                    $worker = new self($queues);
                    $worker->setId($workerId);
                    //$worker->logger = $worker->getLogger($workerId);
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

        Resque::setEventLoop($loop);

        parent::__construct($options['queue']);
    }


    public function work($interval = 5)
    {
        if ($this->shutdown) {
            $this->unregisterWorker()->then(function () {
                $this->loop->stop();
            });
            return;
        }

        $this->reserve()->then(
            function ($response) {
                if (!$response)
                    return;

                list($name, $payload) = $response;

                $names     = explode(':', $name);
                $queueName = array_pop($names);

                echo sprintf(
                        '%s:%s in %s at %d', $queueName, $payload, __FILE__, __LINE__
                    ) . PHP_EOL;

                $job = new Job($queueName, json_decode($payload, true));

                $this->perform($job);

            },
            function (\Exception $e) use ($interval) {
                echo $e->getMessage() . PHP_EOL;
                $this->shutdown();
                $this->work($interval);
            }
        )->then(function () use ($interval) {

            $this->work($interval);

        }, function (\Exception $e) use ($interval) {

            echo $e->getMessage().PHP_EOL;

            $this->shutdown();
            $this->work($interval);
        });
    }

    public function run($interval = 5)
    {
        $this->startup()->then(function () use ($interval) {
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
        $startTime = microtime(true);

        $job->worker = $this;

        echo sprintf('%s in %s at %d', get_class($job), __FILE__, __LINE__) . PHP_EOL;

        return $job->perform()->then(function () use ($job, $startTime) {

            $this->log(array('message' => 'done ID:' . $job->payload['id'], 'data' => array('type' => 'done', 'job_id' => $job->payload['id'], 'time' => round(microtime(true) - $startTime, 3) * 1000)), self::LOG_TYPE_INFO);

        }, function (\Exception $e) use ($job, $startTime) {

            $this->log(array('message' => $job . ' failed: ' . $e->getMessage(), 'data' => array('type' => 'fail', 'log' => $e->getMessage(), 'job_id' => $job->payload['id'], 'time' => round(microtime(true) - $startTime, 3) * 1000)), self::LOG_TYPE_ERROR);
            $job->fail($e);
            $job->updateStatus(\Resque_Job_Status::STATUS_COMPLETE);

        });
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
        //$this->log(array('message' => 'Starting worker ' . $this, 'data' => array('type' => 'start', 'worker' => (string) $this)), self::LOG_TYPE_INFO);
        $this->registerSigHandlers();
        $this->pruneDeadWorkers();
//        Resque_Event::trigger('beforeFirstFork', $this);

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
        if (is_object($this->currentJob)) {
            $this->currentJob->fail(new \Resque_Job_DirtyExitException());
        }

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

        self::all()->then(function ($worker) use ($workerPids) {
            echo sprintf('%s in %s at %d', gettype($worker), __FILE__, __LINE__) . PHP_EOL;

            if (!($worker instanceof Worker))
                return;

            list($host, $pid, $queues) = explode(':', (string)$worker, 3);
            if ($host != $this->hostname || in_array($pid, $workerPids) || $pid == getmypid()) {
                return;
            }
            //$this->log(array('message' => 'Pruning dead worker: ' . (string)$worker, 'data' => array('type' => 'prune')), self::LOG_TYPE_DEBUG);
            $worker->unregisterWorker();
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


}