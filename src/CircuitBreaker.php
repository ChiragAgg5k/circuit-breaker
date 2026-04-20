<?php

namespace ChiragAgg5k;

class CircuitBreaker
{
    private CircuitState $state = CircuitState::CLOSED;
    private int $failures = 0;
    private int $successes = 0;
    private ?int $openedAt = null;

    public function __construct(
        private int $threshold = 3,
        private int $timeout = 30,
        private int $successThreshold = 2
    ) {
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
        if ($this->state === CircuitState::OPEN && $this->hasTimedOut()) {
            $this->transitionToHalfOpen();
        }
    }

    private function onSuccess(): void
    {
        if ($this->state === CircuitState::HALF_OPEN) {
            $this->successes++;

            if ($this->successes >= $this->successThreshold) {
                $this->transitionToClosed();
            }
        } elseif ($this->state === CircuitState::CLOSED) {
            // Reset failure count on success in closed state
            $this->failures = 0;
        }
    }

    private function onFailure(): void
    {
        $this->failures++;

        if ($this->state === CircuitState::HALF_OPEN) {
            // Immediately reopen on failure in half-open state
            $this->transitionToOpen();
        } elseif ($this->failures >= $this->threshold) {
            $this->transitionToOpen();
        }
    }

    private function hasTimedOut(): bool
    {
        return $this->openedAt && (time() - $this->openedAt) >= $this->timeout;
    }

    private function transitionToOpen(): void
    {
        $this->state = CircuitState::OPEN;
        $this->openedAt = time();
        $this->successes = 0;
    }

    private function transitionToHalfOpen(): void
    {
        $this->state = CircuitState::HALF_OPEN;
        $this->failures = 0;
        $this->successes = 0;
    }

    private function transitionToClosed(): void
    {
        $this->state = CircuitState::CLOSED;
        $this->failures = 0;
        $this->successes = 0;
        $this->openedAt = null;
    }

    public function getState(): CircuitState
    {
        $this->updateState();
        return $this->state;
    }

    public function getFailureCount(): int
    {
        return $this->failures;
    }

    public function getSuccessCount(): int
    {
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
