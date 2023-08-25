<?php

namespace Laravel\Horizon;

use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Jobs\SqsJob;
use Illuminate\Queue\SqsQueue as BaseQueue;
use Illuminate\Support\Str;
use Laravel\Horizon\Events\JobPushed;
use Laravel\Horizon\Events\JobReleased;

/**
 * Create a Sqs implementation of the horizon RedisQueue.
 */
class SqsQueue extends BaseQueue
{
    /**
     * The job that last pushed to queue via the "push" method.
     *
     * @var object|string
     */
    protected $lastPushed;

    /**
     * Get the number of queue jobs that are ready to process.
     *
     * @param string|null $queue
     * @return int
     */
    public function readyNow($queue = null)
    {
        return $this->getSqs()->getQueueAttributes([
            'QueueUrl' => $this->getQueue($queue),
            'AttributeNames' => ['ApproximateNumberOfMessages'],
        ])['Attributes']['ApproximateNumberOfMessages'];
    }

    /**
     * Push a new job onto the queue.
     *
     * @param object|string $job
     * @param mixed $data
     * @param string|null $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $queue ?: $this->default, $data),
            $queue,
            null,
            function ($payload, $queue) use ($job) {
                $this->lastPushed = $job;

                return $this->pushRaw($payload, $queue);
            }
        );
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param string $payload
     * @param string|null $queue
     * @param array $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $payload = (new JobPayload($payload))->prepare($this->lastPushed);

        parent::pushRaw($payload->value, $queue, $options);

        $this->event($queue, new JobPushed($payload->value));

        return $payload->id();
    }

    /**
     * Create a payload string from the given job and data.
     *
     * @param string $job
     * @param string $queue
     * @param mixed $data
     * @return array
     */
    protected function createPayloadArray($job, $queue, $data = ''): array
    {
        $payload = parent::createPayloadArray($job, $queue, $data);

        $payload['id'] = $payload['uuid'];

        return $payload;
    }

    /**
     * Push a new job onto the queue after (n) seconds.
     *
     * @param \DateTimeInterface|\DateInterval|int $delay
     * @param string $job
     * @param mixed $data
     * @param string|null $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        $payload = (new JobPayload(
            $this->createPayload($job, $queue ?: $this->default, $data))
        )->prepare($job)->value;

        if (method_exists($this, 'enqueueUsing')) {
            return $this->enqueueUsing(
                $job,
                $payload,
                $queue,
                $delay,
                function ($payload, $queue, $delay) {
                    return tap($this->sqs->sendMessage([
                        'QueueUrl' => $this->getQueue($queue),
                        'MessageBody' => $payload,
                        'DelaySeconds' => $this->secondsUntil($delay),
                    ])->get('MessageId'), function () use ($payload, $queue) {
                        $this->event($this->getQueue($queue), new JobPushed($payload));
                    });
                }
            );
        }

        return tap($this->sqs->sendMessage([
            'QueueUrl' => $this->getQueue($queue),
            'MessageBody' => $payload,
            'DelaySeconds' => $this->secondsUntil($delay),
        ])->get('MessageId'), function () use ($payload, $queue) {
            $this->event($this->getQueue($queue), new JobPushed($payload));
        });
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string|null $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        return tap(parent::pop($queue), function ($result) use ($queue) {
            if ($result instanceof SqsJob) {
                $this->event($this->getQueue($queue), new JobPushed($result->getRawBody()));
            }
        });
    }

    /**
     * Migrate the delayed jobs that are ready to the regular queue.
     *
     * @param string $from
     * @param string $to
     * @return void
     */
    public function migrateExpiredJobs($from, $to)
    {
        // There is job migrations implementation for SQS.
    }

    /**
     * Delete a reserved job from the queue.
     *
     * @param string $queue
     * @param \Illuminate\Queue\Jobs\SqsJob $job
     * @return void
     */
    public function deleteReserved($queue, $job)
    {
        // There is no reserved job implementation for SQS.
    }

    /**
     * Delete a reserved job from the reserved queue and release it.
     *
     * @param  string  $queue
     * @param  \Illuminate\Queue\Jobs\SqsJob  $job
     * @param  int  $delay
     * @return void
     */
    public function deleteAndRelease($queue, $job, $delay)
    {
        // There is no reserved job implementation for SQS.
    }

    /**
     * Fire the given event if a dispatcher is bound.
     *
     * @param string $queue
     * @param mixed $event
     * @return void
     */
    protected function event(string $queue, $event)
    {
        if ($this->container && $this->container->bound(Dispatcher::class)) {
            $queue = Str::replaceFirst('queues:', '', $queue);

            $this->container->make(Dispatcher::class)->dispatch(
                $event->connection($this->getConnectionName())->queue($queue)
            );
        }
    }
}
