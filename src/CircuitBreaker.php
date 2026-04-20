<?php

namespace ChiragAgg5k;

use ChiragAgg5k\CircuitBreaker\Adapter;

class CircuitBreaker
{
    private const STATE_FIELD = 'state';
    private const FAILURES_FIELD = 'failures';
    private const SUCCESSES_FIELD = 'successes';
    private const OPENED_AT_FIELD = 'opened_at';

    private CircuitState $state = CircuitState::CLOSED;
    private int $failures = 0;
    private int $successes = 0;
    private ?int $openedAt = null;

    public function __construct(
        private int $threshold = 3,
        private int $timeout = 30,
        private int $successThreshold = 2,
        private ?Adapter $cache = null,
        private string $cacheKey = 'default'
    ) {
        if ($this->cache !== null && $this->cacheKey === '') {
            throw new \InvalidArgumentException('Cache key must not be empty when a cache adapter is configured.');
        }

        $this->syncFromCache();
    }

    public function call(callable $open, callable $close, ?callable $halfOpen = null): mixed
    {
        $this->updateState();

        if ($this->state === CircuitState::OPEN) {
            return $open();
        }

        // Determine which callback to use
        $callback = ($this->state === CircuitState::HALF_OPEN && $halfOpen !== null)
            ? $halfOpen
            : $close;

        try {
            $result = $callback();
            $this->onSuccess();
            return $result;

        } catch (\Throwable $e) {
            $this->onFailure();
            return $open();
        }
    }

    private function updateState(): void
    {
        $this->syncFromCache();

        if ($this->state === CircuitState::OPEN && $this->hasTimedOut()) {
            $this->transitionToHalfOpen();
        }
    }

    private function onSuccess(): void
    {
        if ($this->state === CircuitState::HALF_OPEN) {
            $successes = $this->incrementSuccesses();

            if ($successes >= $this->successThreshold) {
                $this->transitionToClosed();
            }
        } elseif ($this->state === CircuitState::CLOSED) {
            $this->setFailures(0);
        }
    }

    private function onFailure(): void
    {
        $failures = $this->incrementFailures();

        if ($this->state === CircuitState::HALF_OPEN) {
            // Immediately reopen on failure in half-open state
            $this->transitionToOpen();
        } elseif ($failures >= $this->threshold) {
            $this->transitionToOpen();
        }
    }

    private function hasTimedOut(): bool
    {
        return $this->openedAt !== null && (time() - $this->openedAt) >= $this->timeout;
    }

    private function transitionToOpen(): void
    {
        $this->setState(CircuitState::OPEN);
        $this->setOpenedAt(time());
        $this->setSuccesses(0);
    }

    private function transitionToHalfOpen(): void
    {
        $this->setState(CircuitState::HALF_OPEN);
        $this->setFailures(0);
        $this->setSuccesses(0);
    }

    private function transitionToClosed(): void
    {
        $this->setState(CircuitState::CLOSED);
        $this->setFailures(0);
        $this->setSuccesses(0);
        $this->setOpenedAt(null);
    }

    private function syncFromCache(): void
    {
        if ($this->cache === null) {
            return;
        }

        $this->state = $this->loadState();
        $this->failures = $this->loadInteger(self::FAILURES_FIELD);
        $this->successes = $this->loadInteger(self::SUCCESSES_FIELD);
        $this->openedAt = $this->loadNullableInteger(self::OPENED_AT_FIELD);
    }

    private function setState(CircuitState $state): void
    {
        $this->state = $state;
        $this->cache?->set($this->cacheField(self::STATE_FIELD), $state->value);
    }

    private function setFailures(int $failures): void
    {
        $this->failures = $failures;
        $this->cache?->set($this->cacheField(self::FAILURES_FIELD), $failures);
    }

    private function incrementFailures(): int
    {
        if ($this->cache === null) {
            return ++$this->failures;
        }

        return $this->failures = $this->cache->increment($this->cacheField(self::FAILURES_FIELD));
    }

    private function setSuccesses(int $successes): void
    {
        $this->successes = $successes;
        $this->cache?->set($this->cacheField(self::SUCCESSES_FIELD), $successes);
    }

    private function incrementSuccesses(): int
    {
        if ($this->cache === null) {
            return ++$this->successes;
        }

        return $this->successes = $this->cache->increment($this->cacheField(self::SUCCESSES_FIELD));
    }

    private function setOpenedAt(?int $openedAt): void
    {
        $this->openedAt = $openedAt;

        if ($this->cache === null) {
            return;
        }

        $field = $this->cacheField(self::OPENED_AT_FIELD);
        if ($openedAt === null) {
            $this->cache->delete($field);
            return;
        }

        $this->cache->set($field, $openedAt);
    }

    private function loadState(): CircuitState
    {
        if ($this->cache === null) {
            return $this->state;
        }

        $value = $this->cache->get($this->cacheField(self::STATE_FIELD));
        if (!is_string($value)) {
            return CircuitState::CLOSED;
        }

        return CircuitState::tryFrom($value) ?? CircuitState::CLOSED;
    }

    private function loadInteger(string $field): int
    {
        if ($this->cache === null) {
            return 0;
        }

        $value = $this->cache->get($this->cacheField($field));

        return is_numeric($value) ? max(0, (int) $value) : 0;
    }

    private function loadNullableInteger(string $field): ?int
    {
        if ($this->cache === null) {
            return null;
        }

        $value = $this->cache->get($this->cacheField($field));

        return is_numeric($value) ? (int) $value : null;
    }

    private function cacheField(string $field): string
    {
        return $this->cacheKey . ':' . $field;
    }

    public function getState(): CircuitState
    {
        $this->updateState();
        return $this->state;
    }

    public function getFailureCount(): int
    {
        $this->syncFromCache();

        return $this->failures;
    }

    public function getSuccessCount(): int
    {
        $this->syncFromCache();

        return $this->successes;
    }

    public function isOpen(): bool
    {
        return $this->getState() === CircuitState::OPEN;
    }

    public function isClosed(): bool
    {
        return $this->getState() === CircuitState::CLOSED;
    }

    public function isHalfOpen(): bool
    {
        return $this->getState() === CircuitState::HALF_OPEN;
    }
}
