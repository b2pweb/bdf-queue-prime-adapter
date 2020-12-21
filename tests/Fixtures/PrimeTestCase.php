<?php

namespace Bdf\Queue\Tests;

use Bdf\Prime\Prime;
use Bdf\Prime\Types\ArrayObjectType;
use Bdf\Prime\Types\ArrayType;
use Bdf\Prime\Types\JsonType;
use Bdf\Prime\Types\ObjectType;
use Bdf\Prime\Types\TimestampType;
use Bdf\Prime\Types\TypeInterface;

trait PrimeTestCase
{
    /**
     *
     */
    public function configurePrime()
    {
        if (!Prime::isConfigured()) {
            Prime::configure([
//                'logger' => new PsrDecorator(new Logger()),
//                'resultCache' => new \Bdf\Prime\Cache\ArrayCache(),
                'connection' => [
                    'config' => [
                        'test' => [
                            'adapter' => 'sqlite',
                            'memory' => true
                        ],
                    ]
                ],
            ]);

            Prime::service()->types()->register(new JsonType());
            Prime::service()->types()->register(new ArrayObjectType());
            Prime::service()->types()->register(new ObjectType());
            Prime::service()->types()->register(new ArrayType());
            Prime::service()->types()->register(TimestampType::class, TypeInterface::TIMESTAMP);
        }
    }
}