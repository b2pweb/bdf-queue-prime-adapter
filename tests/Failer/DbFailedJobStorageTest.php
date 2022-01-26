<?php

namespace Bdf\Queue\Failer;

use Bdf\Prime\Connection\ConnectionInterface;
use Bdf\Prime\Prime;
use Bdf\Queue\Message\QueuedMessage;
use Bdf\Queue\Tests\PrimeTestCase;
use PHPUnit\Framework\TestCase;

/**
 * @group Bdf_Queue
 * @group Bdf_Queue_Failed
 */
class DbFailedJobStorageTest extends TestCase
{
    use PrimeTestCase;

    /**
     * @var DbFailedJobStorage
     */
    protected $provider;
    
    /**
     * 
     */
    protected function setUp(): void
    {
        $this->configurePrime();

        $this->provider = DbFailedJobStorage::make(Prime::service(), ['connection' => 'test', 'table' => 'failed']);
        $this->provider->schema()->flush();
    }
    
    /**
     * 
     */
    protected function tearDown(): void
    {
        $this->provider->schema()->drop('failed')->flush();
    }
    
    /**
     *
     */
    public function test_connection()
    {
        $this->assertInstanceOf(ConnectionInterface::class, $this->provider->connection());
    }
    
    /**
     *
     */
    public function test_create_log()
    {
        $message = new QueuedMessage();
        $message->setName('job');
        $message->setConnection('queue-connection');
        $message->setQueue('queue');

        $created = FailedJob::create($message, new \Exception('foo'));

        $this->assertSame(null, $created->id);
        $this->assertSame($message->name(), $created->name);
        $this->assertSame($message->connection(), $created->connection);
        $this->assertSame($message->queue(), $created->queue);
        $this->assertSame($message->toQueue(), $created->messageContent);
        $this->assertSame(QueuedMessage::class, $created->messageClass);
        $this->assertSame('foo', $created->error);
        $this->assertSame(0, $created->attempts);
        $this->assertInstanceOf(\DateTime::class, $created->failedAt);
        $this->assertInstanceOf(\DateTime::class, $created->firstFailedAt);
    }

    /**
     *
     */
    public function test_to_message()
    {
        $message = new QueuedMessage();
        $message->setName('job');
        $message->setConnection('queue-connection');
        $message->setQueue('queue');
        $message->setHeaders([
            'failer-failed-at' => new \DateTime(),
        ]);

        $created = FailedJob::create($message, new \Exception('foo'));

        $this->assertEquals($message, $created->toMessage());
    }

    /**
     *
     */
    public function test_store_log()
    {
        $message = new QueuedMessage();
        $message->setName('job');
        $message->setConnection('queue-connection');
        $message->setQueue('queue');

        $created = FailedJob::create($message, new \Exception('foo'));

        $this->provider->store($created);
        $job = $this->provider->find(1);

        $this->assertSame('1', $job->id);
        $this->assertSame($created->name, $job->name);
        $this->assertSame($created->connection, $job->connection);
        $this->assertSame($created->queue, $job->queue);
        $this->assertSame($message->toQueue(), $created->messageContent);
        $this->assertSame(QueuedMessage::class, $created->messageClass);
        $this->assertSame($created->error, $job->error);
        $this->assertSame(0, $job->attempts);
        $this->assertInstanceOf(\DateTime::class, $job->failedAt);
        $this->assertInstanceOf(\DateTime::class, $job->firstFailedAt);

        $this->assertEquals($job, $this->provider->findById(1));
    }

    /**
     * 
     */
    public function test_all()
    {
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue1',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue2',
        ]));

        $jobs = $this->provider->all();
        $jobs->load();
        $collection = $jobs->collection();

        $this->assertSame(2, count($collection));
        $this->assertSame('queue1', $collection[0]->queue);
        $this->assertSame('queue2', $collection[1]->queue);
    }

    /**
     *
     */
    public function test_all_with_cursor()
    {
        $this->provider = DbFailedJobStorage::make(Prime::service(), ['connection' => 'test', 'table' => 'failed'], 1);

        foreach ([0, 1, 2, 3, 4] as $id) {
            $this->provider->store(new FailedJob([
                'connection' => 'queue-connection',
                'queue' => "queue{$id}",
            ]));
        }

        $jobs = $this->provider->all();
        $this->assertSame(5, $jobs->size());

        $i = 0;
        foreach ($jobs as $job) {
            $this->assertSame("queue{$i}", $job->queue);
            $i++;
        }

        $this->assertSame(5, $i);
    }

    /**
     * 
     */
    public function test_forget()
    {
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue1',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue2',
        ]));
        
        $result = $this->provider->forget(1);
        $jobs = $this->provider->all();
        $jobs->load();
        
        $this->assertTrue($result);
        $this->assertSame(1, count($jobs));
    }

    /**
     *
     */
    public function test_delete()
    {
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue1',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue2',
        ]));

        $toDelete = $this->provider->findById(1);

        $result = $this->provider->delete($toDelete);
        $jobs = $this->provider->all();
        $jobs->load();

        $this->assertTrue($result);
        $this->assertEquals([$this->provider->findById(2)], iterator_to_array($jobs));

        $this->assertFalse($this->provider->delete($toDelete));
    }

    /**
     *
     */
    public function test_flush()
    {
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue1',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue2',
        ]));
        
        $this->provider->flush();
        
        $this->assertSame(0, count($this->provider->all()));
    }

    /**
     *
     */
    public function test_purge_with_criteria()
    {
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue1',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue2',
        ]));

        $this->assertEquals(1, $this->provider->purge((new FailedJobCriteria())->queue('queue2')));
        $this->assertEquals([$this->provider->findById(1)], iterator_to_array($this->provider->all()));
    }

    /**
     *
     */
    public function test_purge_without_criteria()
    {
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue1',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'queue-connection',
            'queue' => 'queue2',
        ]));

        $this->assertEquals(-1, $this->provider->purge(new FailedJobCriteria()));
        $this->assertEmpty(iterator_to_array($this->provider->all()));
    }

    /**
     * @dataProvider provideSearchCriteria
     */
    public function test_search(FailedJobCriteria $criteria, array $expectedIds)
    {
        $this->provider->store(new FailedJob([
            'connection' => 'conn1',
            'queue' => 'queue1',
            'name' => 'Foo',
            'failedAt' => new \DateTime('2022-01-15 15:02:30'),
            'error' => 'my error',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'conn1',
            'queue' => 'queue2',
            'name' => 'Bar',
            'failedAt' => new \DateTime('2022-01-15 22:14:15'),
            'error' => 'my other error',
        ]));
        $this->provider->store(new FailedJob([
            'connection' => 'conn2',
            'queue' => 'queue2',
            'name' => 'Baz',
            'error' => 'hello world',
            'failedAt' => new \DateTime('2021-12-21 08:00:35'),
        ]));

        $this->assertEqualsCanonicalizing($expectedIds, array_map(function (FailedJob $job) {
            return $job->id;
        }, iterator_to_array($this->provider->search($criteria))));
    }

    public function provideSearchCriteria()
    {
        return [
            'empty' => [new FailedJobCriteria(), [1, 2, 3]],
            'connection' => [(new FailedJobCriteria())->connection('conn1'), [1, 2]],
            'queue' => [(new FailedJobCriteria())->queue('queue2'), [2, 3]],
            'name' => [(new FailedJobCriteria())->name('Foo'), [1]],
            'name wildcard' => [(new FailedJobCriteria())->name('Ba*'), [2, 3]],
            'error' => [(new FailedJobCriteria())->error('my error'), [1]],
            'error wildcard' => [(new FailedJobCriteria())->error('my*error'), [1, 2]],
            'error wildcard contains' => [(new FailedJobCriteria())->error('*or*'), [1, 2, 3]],
            'failedAt with date' => [(new FailedJobCriteria())->failedAt(new \DateTime('2022-01-15 22:14:15')), [2]],
            'failedAt with string' => [(new FailedJobCriteria())->failedAt('2022-01-15 22:14:15'), [2]],
            'failedAt with wildcard' => [(new FailedJobCriteria())->failedAt('2022-01-15*'), [1, 2]],
            'failedAt with operator' => [(new FailedJobCriteria())->failedAt('2022-01-01', '<'), [3]],
            'failedAt with operator on value' => [(new FailedJobCriteria())->failedAt('> 2022-01-01'), [1, 2]],
            'queue + connection' => [(new FailedJobCriteria())->connection('conn1')->queue('queue2'), [2]],
            'none match' => [(new FailedJobCriteria())->name('Foo')->error('*world*'), []],
        ];
    }
}
