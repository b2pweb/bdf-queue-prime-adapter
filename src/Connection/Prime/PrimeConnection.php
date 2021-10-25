<?php

namespace Bdf\Queue\Connection\Prime;

use Bdf\Prime\Connection\ConnectionInterface;
use Bdf\Prime\ConnectionManager;
use Bdf\Prime\Schema\Builder\TypesHelperTableBuilder;
use Bdf\Prime\Schema\SchemaManagerInterface;
use Bdf\Queue\Connection\ConnectionDriverInterface;
use Bdf\Queue\Connection\Extension\ConnectionNamed;
use Bdf\Queue\Connection\ManageableQueueInterface;
use Bdf\Queue\Connection\QueueDriverInterface;
use Bdf\Queue\Connection\TopicDriverInterface;
use Bdf\Queue\Exception\MethodNotImplementedException;
use Bdf\Queue\Message\MessageSerializationTrait;
use Bdf\Queue\Serializer\SerializerInterface;

/**
 * Driver using Prime DBAL
 */
class PrimeConnection implements ConnectionDriverInterface, ManageableQueueInterface
{
    use ConnectionNamed;
    use MessageSerializationTrait;

    /**
     * The prime connection manager
     *
     * @var ConnectionManager
     */
    private $connections;

    /**
     * @var array
     */
    private $config;


    /**
     * Create a new connector instance.
     *
     * @param string $name The connection name
     * @param SerializerInterface $serializer
     * @param ConnectionManager $connections
     */
    public function __construct(string $name, SerializerInterface $serializer, ConnectionManager $connections)
    {
        $this->setName($name);
        $this->setSerializer($serializer);
        $this->connections = $connections;
    }

    /**
     * Get the connection manager
     * 
     * @return ConnectionManager
     */
    public function connections()
    {
        return $this->connections;
    }

    /**
     * {@inheritdoc}
     */
    public function setConfig(array $config): void
    {
        $this->config = $config + [
            'host'       => null,
            'table'      => null,
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function config(): array
    {
        return $this->config;
    }

    /**
     * {@inheritdoc}
     */
    public function declareQueue(string $queue): void
    {
        $schema = $this->connection()->schema();

        if (!$schema->hasTable($this->config['table'])) {
            $this->schema();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteQueue(string $queue): void
    {
        // No queue deletion: this will delete all the queues.
        // Acceptable in the context of setup.
        $this->dropSchema();
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {

    }

    /**
     * @return ConnectionInterface
     */
    public function connection()
    {
        return $this->connections->getConnection($this->config['host']);
    }

    /**
     * Get query builder from the table
     * 
     * @return \Bdf\Prime\Query\QueryInterface
     */
    public function table()
    {
        return $this->connection()->from($this->config['table']);
    }

    /**
     * Create Schema
     *
     * @return SchemaManagerInterface
     */
    public function schema()
    {
        return $this->connection()->schema()
            //->simulate() // autoflush ou buffered ?
            ->table($this->config['table'], function(TypesHelperTableBuilder $table) {
                $table->bigint('id')->autoincrement();
                $table->string('queue', 90);
                $table->text('raw');
                $table->boolean('reserved');
                $table->dateTime('reserved_at')->nillable();
                $table->dateTime('available_at');
                $table->dateTime('created_at');
                $table->primary('id');

                $table->index(['queue', 'reserved']);
            });
    }

    /**
     * Drop Schema
     *
     * @return SchemaManagerInterface
     */
    public function dropSchema()
    {
        return $this->schema()->drop($this->config['table']);
    }

    /**
     * {@inheritdoc}
     */
    public function queue(): QueueDriverInterface
    {
        return new PrimeQueue($this);
    }

    /**
     * {@inheritdoc}
     */
    public function topic(): TopicDriverInterface
    {
        throw new MethodNotImplementedException('Topic is not implemented on Prime driver');
    }
}
