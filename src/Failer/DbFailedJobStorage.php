<?php

namespace Bdf\Queue\Failer;

use Bdf\Prime\Connection\ConnectionInterface;
use Bdf\Prime\Query\Contract\Orderable;
use Bdf\Prime\Query\Pagination\Walker;
use Bdf\Prime\Query\Pagination\WalkStrategy\KeyWalkStrategy;
use Bdf\Prime\Schema\Builder\TypesHelperTableBuilder;
use Bdf\Prime\Schema\SchemaManagerInterface;
use Bdf\Prime\ServiceLocator;
use Bdf\Prime\Types\TypeInterface;
use Bdf\Queue\Failer\Walker\FailedJobPrimaryKey;

/**
 * DbFailedJobStorage
 */
class DbFailedJobStorage implements FailedJobStorageInterface
{
    const DEFAULT_MAX_ROWS = 50;

    /**
     * The prime connection
     *
     * @var ConnectionInterface
     */
    private $connection;
    
    /**
     * The schema info
     *
     * @var array
     */
    private $schema;

    /**
     * The row size of the cursor for the prime walker
     *
     * @var int
     */
    private $maxRows;

    /**
     * Create a new database failed job provider.
     *
     * @param ServiceLocator $locator
     * @param array $schema
     * @param int $maxRows
     * @return static
     */
    public static function make(ServiceLocator $locator, array $schema, int $maxRows = self::DEFAULT_MAX_ROWS)
    {
        return new static($locator->connection($schema['connection']), $schema, $maxRows);
    }

    /**
     * Create a new database failed job provider.
     *
     * @param ConnectionInterface $connection
     * @param array $schema
     * @param int $maxRows
     */
    public function __construct(ConnectionInterface $connection, array $schema, int $maxRows = self::DEFAULT_MAX_ROWS)
    {
        $this->connection = $connection;
        $this->schema = $schema;
        $this->maxRows = $maxRows;
    }

    /**
     * Get the connection.
     *
     * @return ConnectionInterface
     */
    public function connection()
    {
        return $this->connection;
    }

    /**
     * {@inheritdoc}
     */
    public function store(FailedJob $job)
    {
        $this->connection->insert($this->schema['table'], [
            'name' => (string)$job->name,
            'connection' => (string)$job->connection,
            'queue' => (string)$job->queue,
            'messageClass' => $job->messageClass,
            'messageContent' => $job->messageContent,
            'error' => $job->error,
            'attempts' => (int)$job->attempts,
            'failed_at' => $job->failedAt,
            'first_failed_at' => $job->firstFailedAt,
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function all()
    {
        $walker = $this->connection->from($this->schema['table'])
            ->post([$this, 'postProcessor'])
            /*
             * Set a limit on date to not load futur failed message
             */
            ->where('failed_at', '<=', new \DateTime())
            /*
             * Use an order by id than failed_at for a functional purpose
             * The key walk strategy works only if the cursor is ordered on a unique field.
             */
            ->order('id', Orderable::ORDER_ASC)
            ->walk($this->maxRows);

        if ($walker instanceof Walker) {
            $walker->setStrategy(new KeyWalkStrategy(new FailedJobPrimaryKey()));
        }

        return $walker;
    }

    /**
     * {@inheritdoc}
     */
    public function find($id)
    {
        return $this->connection->from($this->schema['table'])
            ->post([$this, 'postProcessor'])
            ->where('id', $id)
            ->first();
    }

    /**
     * {@inheritdoc}
     */
    public function forget($id)
    {
        return $this->connection->delete($this->schema['table'], ['id' => $id]) > 0;
    }

    /**
     * {@inheritdoc}
     */
    public function flush()
    {
        $this->connection->schema()->truncate($this->schema['table']);
    }
    
    /**
     * Create Schema
     *
     * The schema is buffered
     *
     * @return SchemaManagerInterface
     */
    public function schema()
    {
        return $this->connection->schema()
            ->simulate()
            ->table($this->schema['table'], function(TypesHelperTableBuilder $table) {
                $table->bigint('id')->autoincrement();
                $table->string('name', 255);
                $table->string('connection', 90);
                $table->string('queue', 90);
                $table->string('messageClass');
                $table->integer('attempts', 0);
                $table->arrayObject('messageContent')->nillable();
                $table->text('error')->nillable();
                $table->dateTime('failed_at');
                $table->dateTime('first_failed_at');
                $table->primary('id');
                $table->index('failed_at');
                $table->index('queue');
                $table->index('attempts');
            });
    }
    
    /**
     * Internal method. Format the db row on post process
     * 
     * @param array $row
     * 
     * @return FailedJob
     */
    public function postProcessor($row)
    {
        $job = new FailedJob();
        $job->id = $row['id'];
        $job->name = $row['name'];
        $job->connection = $row['connection'];
        $job->queue = $row['queue'];
        $job->error = $row['error'];
        $job->attempts = (int)$row['attempts'];
        $job->messageClass = $row['messageClass'] ?? null;
        $job->messageContent = $this->connection->fromDatabase($row['messageContent'] ?? null, TypeInterface::ARRAY_OBJECT);
        $job->failedAt = $this->connection->fromDatabase($row['failed_at'], TypeInterface::DATETIME);
        // For jobs have not been migrated: we set the failed at date by default.
        $job->firstFailedAt = $this->connection->fromDatabase($row['first_failed_at'], TypeInterface::DATETIME) ?? $job->failedAt;

        return $job;
    }
}
