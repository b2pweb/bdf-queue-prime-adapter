<?php

namespace Bdf\Queue\Console\Command\Failer;

use Bdf\Queue\Failer\DbFailedJobStorage;
use Bdf\Queue\Failer\FailedJobStorageInterface;
use Bdf\Util\Console\BdfStyle;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * InitCommand
 */
#[AsCommand('queue:prime:failer:init', 'Create or update db table failer.')]
class InitCommand extends Command
{
    protected static $defaultName = 'queue:prime:failer:init';

    /**
     * @var FailedJobStorageInterface
     */
    private $failer;

    /**
     * SetupCommand constructor.
     *
     * @param FailedJobStorageInterface $failer
     */
    public function __construct(FailedJobStorageInterface $failer)
    {
        $this->failer = $failer;

        parent::__construct(static::$defaultName);
    }

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this
            ->setDescription('Create or update db table failer.')
            ->addOption('execute',   null, InputOption::VALUE_NONE, 'Execute the queries')
        ;
    }
    
    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new BdfStyle($input, $output);

        if (!$this->failer instanceof DbFailedJobStorage) {
            $io->alert('Cannot initialize failure if not an instance of db failure');
            return -1;
        }

        $schema = $this->failer->schema();
        $queries = $schema->pending();
        
        if (!$queries) {
            $io->comment("failed job is up to date");
            return 0;
        }

        $io->comment('failed job needs upgrade');

        foreach ($queries as $query) {
            $io->line($query);
        }

        if ($io->option('execute')) {
            $io->line('launching query ', ' ');

            try {
                $schema->flush();

                $io->info('[OK]');
            } catch (\Exception $e) {
                $io->alert($e->getMessage());
            }
        }
        
        $schema->clear();

        return 0;
    }
}
