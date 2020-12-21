<?php

namespace Bdf\Queue\Connection\Prime;

use Bdf\Prime\Prime;
use Bdf\Queue\Exception\MethodNotImplementedException;
use Bdf\Queue\Serializer\JsonSerializer;
use Bdf\Queue\Tests\PrimeTestCase;
use PHPUnit\Framework\TestCase;

/**
 *
 */
class PrimeConnectionTest extends TestCase
{
    use PrimeTestCase;

    /**
     * @var PrimeConnection
     */
    protected $connection;
    
    /**
     * 
     */
    protected function setUp(): void
    {
        $this->configurePrime();
        
        $this->connection = new PrimeConnection('name', new JsonSerializer(), Prime::service()->connections());
        $this->connection->setConfig(['host' => 'test', 'table' => 'job']);
        $this->connection->schema();
    }
    
    /**
     * 
     */
    protected function tearDown(): void
    {
        $this->connection->dropSchema();
    }
    
    /**
     * 
     */
    public function test_connections()
    {
        $this->assertSame(Prime::service()->connections(), $this->connection->connections());
    }
    
    /**
     * 
     */
    public function test_unused_methods()
    {
        $this->assertNull($this->connection->setConfig(['host' => 'test', 'table' => 'job']));
        $this->assertNull($this->connection->close());
    }

    /**
     *
     */
    public function test_queue()
    {
        $this->assertInstanceOf(PrimeQueue::class, $this->connection->queue());
    }

    /**
     *
     */
    public function test_topic()
    {
        $this->expectException(MethodNotImplementedException::class);

        $this->connection->topic();
    }
}
