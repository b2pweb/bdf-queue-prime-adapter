<?php

namespace Bdf\Queue\Failer\Walker;


use Bdf\Prime\Query\Pagination\WalkStrategy\KeyInterface;
use Bdf\Queue\Failer\FailedJob;

/**
 * Extract the primary key of FailedJob instance
 *
 * @template E as object
 * @implements KeyInterface<E>
 */
final class FailedJobPrimaryKey implements KeyInterface
{
    /**
     * {@inheritdoc}
     */
    public function name(): string
    {
        return 'id';
    }

    /**
     * {@inheritdoc}
     */
    public function get($entity)
    {
        if (!$entity instanceof FailedJob) {
            throw new \LogicException('The Failed job key extractor works only with FailedJob instance. '.gettype($entity). ' given.');
        }

        return $entity->id;
    }
}
