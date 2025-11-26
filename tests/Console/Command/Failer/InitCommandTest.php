<?php

namespace Bdf\Queue\Console\Command\Failer;

use Bdf\Queue\Failer\FailedJobStorageInterface;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

use function method_exists;

/**
 *
 */
class InitCommandTest extends TestCase
{
    /**
     * 
     */
    public function test_no_db_failer()
    {
        $failer = $this->createMock(FailedJobStorageInterface::class);

        $command = new InitCommand($failer);
        $tester = new CommandTester($command);
        $tester->execute([]);

        if (method_exists($this, 'assertMatchesRegularExpression')) {
            $this->assertMatchesRegularExpression('/Cannot initialize failure if not an instance of db failure/', $tester->getDisplay());
        } else {
            $this->assertRegExp('/Cannot initialize failure if not an instance of db failure/', $tester->getDisplay());
        }
    }
}