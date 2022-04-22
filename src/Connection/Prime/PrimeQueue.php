<?php

namespace Bdf\Queue\Connection\Prime;

use Bdf\Prime\Query\Expression\Raw;
use Bdf\Prime\Types\TypeInterface;
use Bdf\Queue\Connection\ConnectionDriverInterface;
use Bdf\Queue\Connection\CountableQueueDriverInterface;
use Bdf\Queue\Connection\Extension\EnvelopeHelper;
use Bdf\Queue\Connection\PeekableQueueDriverInterface;
use Bdf\Queue\Connection\QueueDriverInterface;
use Bdf\Queue\Connection\ReservableQueueDriverInterface;
use Bdf\Queue\Message\EnvelopeInterface;
use Bdf\Queue\Message\Message;
use Bdf\Queue\Message\QueuedMessage;
use DateTime;

/**
 * Queue driver for PrimeConnection
 */
class AbstractPrimeQueue implements QueueDriverInterface, ReservableQueueDriverInterface, PeekableQueueDriverInterface
{
    use EnvelopeHelper;

    /**
     * @var PrimeConnection
     */
    private $connection;


    /**
     * PrimeQueue constructor.
     *
     * @param PrimeConnection $connection
     */
    public function __construct(PrimeConnection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function connection(): ConnectionDriverInterface
    {
        return $this->connection;
    }

    /**
     * {@inheritdoc}
     */
    public function push(Message $message): void
    {
        $message->setQueuedAt(new \DateTimeImmutable());

        $this->pushRaw($this->connection->serializer()->serialize($message), $message->queue(), $message->delay());
    }

    /**
     * {@inheritdoc}
     */
    public function pushRaw($raw, string $queue, int $delay = 0): void
    {
        $this->connection->table()->insert($this->entity($delay, $queue, $raw));
    }

    /**
     * {@inheritdoc}
     */
    public function pop(string $queue, int $duration = ConnectionDriverInterface::DURATION): ?EnvelopeInterface
    {
        $messages = $this->reserve(1, $queue, $duration);

        return $messages[0] ?? null;
    }

    /**
     * {@inheritdoc}
     */
    public function reserve(int $number, string $queue, int $duration = ConnectionDriverInterface::DURATION): array
    {
        $dbJobs = $this->connection->connection()->transactional(function() use($number, $queue) {
            $dbJobs = $this->select($queue)->limit($number)->all();

            if (!$dbJobs) {
                return $dbJobs;
            }

            $ids = [];
            foreach ($dbJobs as $job) {
                $ids[] = $job['id'];
            }

            $this->connection->table()->where('id', ':in', $ids)->update([
                'reserved' => true,
                'reserved_at' => new DateTime(),
            ]);

            return $dbJobs;
        });

        // Message instantiation: this creation is done after le transaction
        // This allows the lock to be removed asap
        $messages = [];
        foreach ($dbJobs as $job) {
            $messages[] = $this->toQueueEnvelope($this->connection->toQueuedMessage($job['raw'], $job['queue'], $job));
        }

        // Force sleep if no result found
        if (!isset($messages[0])) {
            sleep($duration);
        }

        return $messages;
    }

    /**
     * {@inheritdoc}
     */
    public function acknowledge(QueuedMessage $message): void
    {
        $this->connection->table()->where('id', $message->internalJob()['id'])->delete();
    }

    /**
     * {@inheritdoc}
     */
    public function release(QueuedMessage $message): void
    {
        $data = ['reserved' => false];

        if ($message->delay() > 0) {
            $data['available_at'] = new DateTime("+{$message->delay()} seconds");
        }

        $this->connection->table()->where('id', $message->internalJob()['id'])->update($data);
    }

    /**
     * {@inheritdoc}
     */
    public function count(string $queueName): int
    {
        return $this->connection->table()->where('queue', $queueName)->count();
    }

    /**
     * {@inheritdoc}
     */
    public function peek(string $queueName, int $rowCount = 20, int $page = 1): array
    {
        $dbJobs = $this->connection->table()
            ->post([$this, 'postProcessor'])
            ->order('id')
            ->where('queue', $queueName)
            ->limitPage($page, $rowCount)
            ->all();

        $messages = [];
        foreach ($dbJobs as $job) {
            $messages[] = $this->connection->toQueuedMessage($job['raw'], $job['queue'], $job);
        }

        return $messages;
    }

    /**
     * {@inheritdoc}
     */
    public function stats(): array
    {
        $stats = [];
        $queueReserved = [];
        $queueDelayed = [];

        // Get running job by queue and group result by queue
        $result = $this->connection->table()->group('queue')->where('reserved', true)->all(['queue', new Raw('count(*) as nb')]);
        foreach ($result as $row) {
            $queueReserved[$row['queue']] = (int)$row['nb'];
        }

        // Get delayed job by queue and group result by queue
        $result = $this->connection->table()->group('queue')->where('available_at', '>', new DateTime())->all(['queue', new Raw('count(*) as nb')]);
        foreach ($result as $row) {
            $queueDelayed[$row['queue']] = (int)$row['nb'];
        }
        
        // Get all job by queue
        $result = $this->connection->table()->group('queue')->all(['queue', new Raw('count(*) as nb')]);
        
        // build stats result.
        foreach ($result as $row) {
            $queue = $row['queue'];
            
            if (!isset($queueReserved[$queue])) {
                $queueReserved[$queue] = 0;
            }
            if (!isset($queueDelayed[$queue])) {
                $queueDelayed[$queue] = 0;
            }

            $stats[] = [
                'queue'         => $row['queue'], 
                'jobs in queue' => (int)$row['nb'],
                'jobs awaiting' => $row['nb'] - $queueReserved[$queue] - $queueDelayed[$queue],
                'jobs running'  => $queueReserved[$queue],
                'jobs delayed'  => $queueDelayed[$queue],
            ];
        }
        
        return ['queues' => $stats];
    }

    /**
     * Get query builder for selection
     *
     * @return \Bdf\Prime\Query\Query
     */
    public function select($queue)
    {
        return $this->connection->table()
            ->post([$this, 'postProcessor'])
            ->order('id')
            ->where('queue', $queue)
            ->where('reserved', false)
            ->where('available_at', '<=', new DateTime())
            ->lock();
    }

    /**
     * Internal method. Format the db row on post process
     * 
     * @param array $row
     * 
     * @return array
     */
    public function postProcessor($row)
    {
        $connection = $this->connection->connection();

        $row['created_at'] = $connection->fromDatabase($row['created_at'], TypeInterface::DATETIME);
        $row['available_at'] = $connection->fromDatabase($row['available_at'], TypeInterface::DATETIME);
        $row['reserved_at'] = $connection->fromDatabase($row['reserved_at'], TypeInterface::DATETIME);
        
        return $row;
    }

    /**
     * Push a data to the connection with a given delay.
     *
     * @param int $delay
     * @param string $queue
     * @param string $raw
     *
     * @return array
     */
    private function entity($delay, $queue, $raw)
    {
        $available = new DateTime();

        if ($delay > 0) {
            $available->modify($delay.' second');
        }

        return [
            'queue'         => $queue,
            'raw'           => $raw,
            'reserved'      => 0,
            'reserved_at'   => null,
            'available_at'  => $available,
            'created_at'    => new DateTime(),
        ];
    }
}

if (interface_exists(CountableQueueDriverInterface::class)) {
    class PrimeQueue extends AbstractPrimeQueue implements CountableQueueDriverInterface
    {

    }
} else {
    class PrimeQueue extends AbstractPrimeQueue
    {

    }
}
