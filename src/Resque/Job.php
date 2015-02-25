<?php
/**
 * Job.php
 *
 * @copyright   Copyright (c) 2014 sonicmoov Co.,Ltd.
 * @version     $Id$
 *
 */

namespace Iwai\React\Resque;

use Iwai\React\Resque;
use React\Promise\PromiseInterface;
use Resque_Job;

class Job extends \Resque_Job {

    /**
     * @var string The name of the queue that this job belongs to.
     */
    public $queue;

    /**
     * @var Worker Instance of the Resque worker running this job.
     */
    public $worker;

    /**
     * @var object Object containing details of the job.
     */
    public $payload;

    /**
     * @var object Instance of the class performing work for this job.
     */
    private $instance;

    static private $handler;

    /**
     * Instantiate a new instance of a job.
     *
     * @param string $queue The queue that the job belongs to.
     * @param array $payload array containing details of the job.
     */
    public function __construct($queue, $payload)
    {
        $this->queue = $queue;
        $this->payload = $payload;
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     * @param boolean $monitor Set to true to be able to monitor the status of a job.
     *
     * @return string
     */
    public static function create($queue, $class, $args = null, $monitor = false)
    {
        if ($args !== null && !is_array($args)) {
            throw new \InvalidArgumentException(
                'Supplied $args must be an array.'
            );
        }

        $new = true;
        if(isset($args['id'])) {
            $id = $args['id'];
            unset($args['id']);
            $new = false;
        } else {
            $id = md5(uniqid('', true));
        }
        Resque::push($queue, array(
            'class'	=> $class,
            'args'	=> array($args),
            'id'	=> $id,
        ));

        if ($monitor) {
            if ($new) {
                \Resque_Job_Status::create($id);
            } else {
                $statusInstance = new \Resque_Job_Status($id);
                $statusInstance->update($id, \Resque_Job_Status::STATUS_WAITING);
            }
        }

        return $id;
    }

    /**
     * Update the status of the current job.
     *
     * @param int $status Status constant from Resque_Job_Status indicating the current status of a job.
     */
    public function updateStatus($status)
    {
        if(empty($this->payload['id'])) {
            return;
        }

        $statusInstance = new \Resque_Job_Status($this->payload['id']);
        $statusInstance->update($status);
    }

    /**
     * Get the arguments supplied to this job.
     *
     * @return array Array of arguments.
     */
    public function getArguments()
    {
        if (!isset($this->payload['args'])) {
            return array();
        }

        return $this->payload['args'][0];
    }

    /**
     * Get the instantiated object for this job that will be performing work.
     *
     * @return object Instance of the object that this job belongs to.
     */
    public function getInstance()
    {
        if (!is_null($this->instance)) {
            return $this->instance;
        }

        if (class_exists('Resque_Job_Creator')) {
            $this->instance = Resque_Job_Creator::createJob($this->payload['class'], $this->getArguments());
        } else {
            if(!class_exists($this->payload['class'])) {
                throw new \Resque_Exception(
                    'Could not find job class ' . $this->payload['class'] . '.'
                );
            }

            if(!method_exists($this->payload['class'], 'perform')) {
                throw new \Resque_Exception(
                    'Job class ' . $this->payload['class'] . ' does not contain a perform method.'
                );
            }
            $this->instance = new $this->payload['class']();
        }

        $this->instance->job = $this;
        $this->instance->args = $this->getArguments();
        $this->instance->queue = $this->queue;

        if (!self::$handler) {
            self::$handler = new \WyriHaximus\React\RingPHP\HttpClientAdapter(
                Resque::getEventLoop()
            );
        }
        $this->instance->handler = self::$handler;

        return $this->instance;
    }

    /**
     * @return \React\Promise\Promise
     */
    public function perform()
    {
        $instance = $this->getInstance();

        $deferred = new \React\Promise\Deferred();

        $promise = $deferred->promise();
        $promise->then(function () {
            $this->worker->processing++;
        });

        if(method_exists($instance, 'setUp')) {
            $promise = $promise->then(function () use ($instance) {
                return \React\Promise\resolve($instance->setUp());
            });
        }

        $promise = $promise->then(function () use ($instance) {
            return $instance->perform();
        });

        if(method_exists($instance, 'tearDown')) {
            $promise = $promise->then(function () use ($instance) {
                return \React\Promise\resolve($instance->tearDown());
            });
        }

        $promise->then(
            function () {
                $this->worker->processing--;
            },
            function ($e) {
                $this->worker->processing--;

                if ($e instanceof \Exception) {
                    error_log(sprintf('%s:%s in %s at %d',
                        get_class($e), $e->getMessage(), __FILE__, __LINE__));
                }
                error_log($e);
            }
        );

        $deferred->resolve();

        return $promise;
    }

    /**
     * Mark the current job as having failed.
     *
     * @param $exception
     */
    public function fail($exception)
    {
        \Resque_Event::trigger('onFailure', array(
            'exception' => $exception,
            'job' => $this,
        ));

        $this->updateStatus(\Resque_Job_Status::STATUS_FAILED);

        \Resque_Failure::create(
            $this->payload,
            $exception,
            $this->worker,
            $this->queue
        );
        \Resque_Stat::incr('failed');
        \Resque_Stat::incr('failed:' . $this->worker);
    }

    /**
     * Re-queue the current job.
     * @return string
     */
    public function recreate()
    {
        $status = new \Resque_Job_Status($this->payload['id']);
        $monitor = false;
        if($status->isTracking()) {
            $monitor = true;
        }

        return self::create($this->queue, $this->payload['class'], $this->payload['args'], $monitor);
    }

} 