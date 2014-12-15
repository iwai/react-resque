<?php
class PHP_Job
{
    public function setUp($promise)
    {
        /** @var \React\Promise\PromiseInterface $promise */
        $promise->then(function ($value = null) {
            echo 'value: ' . gettype($value) . PHP_EOL;
            echo sprintf('%s in %s at %d', get_class($this), __FILE__, __LINE__) . PHP_EOL;
            sleep(1);
        });

        return $promise;
    }

	public function perform($promise)
	{
        /** @var \React\Promise\PromiseInterface $promise */
        $promise->then(function ($value = null) {
            echo 'value: ' . gettype($value) . PHP_EOL;
            echo sprintf('%s in %s at %d', get_class($this), __FILE__, __LINE__) . PHP_EOL;
            sleep(1);
        });

		sleep(3);
        echo sprintf('%s in %s at %d', get_class($this), __FILE__, __LINE__) . PHP_EOL;

        return $promise;
	}

    public function tearDown($promise)
    {

        /** @var \React\Promise\PromiseInterface $promise */
        $promise->then(function ($value = null) {
            echo 'value: ' . gettype($value) . PHP_EOL;
            echo sprintf('%s in %s at %d', get_class($this), __FILE__, __LINE__) . PHP_EOL;
            sleep(1);
        });

        return $promise;
    }

}
?>