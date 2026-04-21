<?php

namespace ChiragAgg5k\Tests\unit;

use ChiragAgg5k\CircuitBreaker;
use ChiragAgg5k\CircuitBreaker\Adapter;
use ChiragAgg5k\CircuitState;
use PHPUnit\Framework\TestCase;
use Utopia\Telemetry\Adapter\Test as TestTelemetry;

final class CircuitBreakerTest extends TestCase
{
    public function testUsesInMemoryStateByDefault(): void
    {
        $breaker = new CircuitBreaker(threshold: 2, timeout: 30, successThreshold: 1);

        $first = $breaker->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );
        $second = $breaker->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );

        self::assertSame('fallback', $first);
        self::assertSame('fallback', $second);
        self::assertSame(CircuitState::OPEN, $breaker->getState());
        self::assertSame(2, $breaker->getFailureCount());
    }

    public function testCachedStateIsSharedAcrossBreakerInstances(): void
    {
        $cache = $this->createArrayAdapter();
        $first = new CircuitBreaker(threshold: 2, timeout: 30, successThreshold: 1, cache: $cache, cacheKey: 'users-api');
        $second = new CircuitBreaker(threshold: 2, timeout: 30, successThreshold: 1, cache: $cache, cacheKey: 'users-api');

        $first->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );
        $first->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );

        self::assertTrue($second->isOpen());
        self::assertSame(2, $second->getFailureCount());

        $result = $second->call(
            open: static fn () => 'shared fallback',
            close: function (): void {
                self::fail('Closed callback should not run while the shared circuit is open.');
            }
        );

        self::assertSame('shared fallback', $result);
    }

    public function testClosedSuccessDoesNotWriteZeroFailuresWhenAlreadyZero(): void
    {
        $cache = new class () implements Adapter {
            /**
             * @var list<array{string, string, int|string|null}>
             */
            public array $writes = [];

            public function get(string $key): int|string|null
            {
                return null;
            }

            public function set(string $key, int|string $value): void
            {
                $this->writes[] = ['set', $key, $value];
            }

            public function increment(string $key, int $by = 1): int
            {
                $this->writes[] = ['increment', $key, $by];

                return $by;
            }

            public function delete(string $key): void
            {
                $this->writes[] = ['delete', $key, null];
            }
        };
        $breaker = new CircuitBreaker(threshold: 1, timeout: 30, successThreshold: 1, cache: $cache, cacheKey: 'users-api');

        self::assertSame('ok', $breaker->call(
            open: static fn () => 'fallback',
            close: static fn () => 'ok'
        ));

        self::assertSame([], $cache->writes);
    }

    public function testCachedTransitionsWriteStateLast(): void
    {
        $cache = new class () implements Adapter {
            /**
             * @var array<string, int|string>
             */
            private array $values = [];

            /**
             * @var list<array{string, string, int|string|null}>
             */
            public array $writes = [];

            public function get(string $key): int|string|null
            {
                return $this->values[$key] ?? null;
            }

            public function set(string $key, int|string $value): void
            {
                $this->writes[] = ['set', $key, $value];
                $this->values[$key] = $value;
            }

            public function increment(string $key, int $by = 1): int
            {
                $value = (int) ($this->values[$key] ?? 0);
                $value += $by;
                $this->writes[] = ['increment', $key, $by];
                $this->values[$key] = $value;

                return $value;
            }

            public function delete(string $key): void
            {
                $this->writes[] = ['delete', $key, null];
                unset($this->values[$key]);
            }
        };
        $breaker = new CircuitBreaker(threshold: 1, timeout: 30, successThreshold: 1, cache: $cache, cacheKey: 'users-api');

        $breaker->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );

        $setWrites = array_values(array_filter(
            $cache->writes,
            static fn (array $write): bool => $write[0] === 'set'
        ));

        self::assertSame(['set', 'users-api:state', CircuitState::OPEN->value], $setWrites[array_key_last($setWrites)]);
    }

    public function testHalfOpenSuccessesCloseTheCircuit(): void
    {
        $breaker = new CircuitBreaker(threshold: 1, timeout: 0, successThreshold: 2);

        $breaker->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );

        self::assertSame('probe-1', $breaker->call(
            open: static fn () => 'fallback',
            close: static fn () => 'closed',
            halfOpen: static fn () => 'probe-1'
        ));
        self::assertTrue($breaker->isHalfOpen());
        self::assertSame(1, $breaker->getSuccessCount());

        self::assertSame('probe-2', $breaker->call(
            open: static fn () => 'fallback',
            close: static fn () => 'closed',
            halfOpen: static fn () => 'probe-2'
        ));

        self::assertTrue($breaker->isClosed());
        self::assertSame(0, $breaker->getFailureCount());
        self::assertSame(0, $breaker->getSuccessCount());
    }

    public function testRecordsTelemetryForCallsFallbacksAndTransitions(): void
    {
        $telemetry = new TestTelemetry();
        $breaker = new CircuitBreaker(threshold: 1, timeout: 30, successThreshold: 1, telemetry: $telemetry);

        $result = $breaker->call(
            open: static fn () => 'fallback',
            close: static function (): void {
                throw new \RuntimeException('failed');
            }
        );

        self::assertSame('fallback', $result);
        self::assertSame([1], $telemetry->counters['circuit_breaker.calls']->values);
        self::assertSame([1], $telemetry->counters['circuit_breaker.callback_failures']->values);
        self::assertSame([1], $telemetry->counters['circuit_breaker.fallbacks']->values);
        self::assertSame([1], $telemetry->counters['circuit_breaker.transitions']->values);
        self::assertSame([1, -1], $telemetry->upDownCounters['circuit_breaker.active_calls']->values);
        self::assertSame([1, 1], $telemetry->gauges['circuit_breaker.state']->values);
        self::assertSame([1, 1], $telemetry->gauges['circuit_breaker.failures']->values);
        self::assertSame([0, 0], $telemetry->gauges['circuit_breaker.successes']->values);
        self::assertCount(1, $telemetry->gauges['circuit_breaker.event.timestamp']->values);
    }

    public function testRejectsEmptyCacheKeyWhenCacheIsConfigured(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new CircuitBreaker(cache: $this->createArrayAdapter(), cacheKey: '');
    }

    private function createArrayAdapter(): Adapter
    {
        return new class () implements Adapter {
            /**
             * @var array<string, int|string>
             */
            private array $values = [];

            public function get(string $key): int|string|null
            {
                return $this->values[$key] ?? null;
            }

            public function set(string $key, int|string $value): void
            {
                $this->values[$key] = $value;
            }

            public function increment(string $key, int $by = 1): int
            {
                $value = (int) ($this->values[$key] ?? 0);
                $value += $by;
                $this->values[$key] = $value;

                return $value;
            }

            public function delete(string $key): void
            {
                unset($this->values[$key]);
            }
        };
    }
}
