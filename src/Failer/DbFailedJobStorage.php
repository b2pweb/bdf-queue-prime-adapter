<?php

namespace Bdf\Queue\Failer;

use Bdf\Prime\Connection\ConnectionInterface;
use Bdf\Prime\Schema\Builder\TypesHelperTableBuilder;
use Bdf\Prime\Schema\SchemaManagerInterface;
use Bdf\Prime\ServiceLocator;
use Bdf\Prime\Types\TypeInterface;

/**
 * DbFailedJobStorage
 */
class DbFailedJobStorage implements FailedJobStorageInterface
{
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
     * Create a new database failed job provider.
     *
     * @param  ServiceLocator  $locator
     * @param  array           $schema
     *
     * @return static
     */
    public static function make(ServiceLocator $locator, array $schema)
    {
        return new static($locator->connection($schema['connection']), $schema);
    }

    /**
     * Create a new database failed job provider.
     *
     * @param  ConnectionInterface  $connection
     * @param  array                $schema
     */
    public function __construct(ConnectionInterface $connection, array $schema)
    {
        $this->connection = $connection;
        $this->schema = $schema;
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
            'failed_at' => $job->failedAt,
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function all()
    {
        return $this->connection->from($this->schema['table'])
            ->post([$this, 'postProcessor'])
            ->order('failed_at', 'asc')
            ->walk(50);
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
                $table->bigint('id', true);
                $table->string('name', 255);
                $table->string('connection', 90);
                $table->string('queue', 90);
                $table->string('messageClass');
                $table->arrayObject('messageContent')->nillable();
                $table->text('error')->nillable();
                $table->dateTime('failed_at');
                $table->primary('id');
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
        $job->messageClass = $row['messageClass'] ?? null;
        $job->messageContent = $this->connection->fromDatabase($row['messageContent'] ?? null, TypeInterface::ARRAY_OBJECT);
        $job->failedAt = $this->connection->fromDatabase($row['failed_at'], TypeInterface::DATETIME);

        return $job;
    }
}
