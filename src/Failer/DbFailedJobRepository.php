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
 * Implementation of failed jobs repository using prime DBAL
 */
class DbFailedJobRepository extends DbFailedJobStorage
{

}
