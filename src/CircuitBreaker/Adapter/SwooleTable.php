<?php

namespace ChiragAgg5k\CircuitBreaker\Adapter;

use ChiragAgg5k\CircuitBreaker\Adapter as CircuitBreakerAdapter;

final class SwooleTable implements CircuitBreakerAdapter
{
    public const VALUE_COLUMN = 'value';
    public const NUMBER_COLUMN = 'number';

    private const DEFAULT_VALUE_LENGTH = 255;
    private const MAX_TABLE_KEY_LENGTH = 63;

    /**
     * @param object $table A Swoole\Table created with value and number columns.
     */
    public function __construct(
        private object $table,
        private string $prefix = 'breaker:'
    ) {
        foreach (['get', 'set', 'incr', 'del'] as $method) {
            if (!method_exists($this->table, $method)) {
                throw new \InvalidArgumentException(sprintf(
                    '%s requires a Swoole table-compatible object with a %s() method.',
                    self::class,
                    $method
                ));
            }
        }
    }

    public static function createTable(
        int $size = 1024,
        int $valueLength = self::DEFAULT_VALUE_LENGTH
    ): object {
        if (!class_exists(\Swoole\Table::class)) {
            throw new AdapterException('The swoole extension is required to create a Swoole table.');
        }

        $table = new \Swoole\Table($size);
        $table->column(self::VALUE_COLUMN, \Swoole\Table::TYPE_STRING, $valueLength);
        $table->column(self::NUMBER_COLUMN, \Swoole\Table::TYPE_INT);
        $table->create();

        return $table;
    }

    public function get(string $key): int|string|null
    {
        $row = $this->command('get', [$this->key($key)]);

        if ($row === false || $row === null) {
            return null;
        }

        if (!is_array($row)) {
            throw new AdapterException(sprintf('Unexpected Swoole table row for cache key "%s".', $key));
        }

        $value = $row[self::VALUE_COLUMN] ?? '';
        if ($value !== '') {
            return (string) $value;
        }

        return (int) ($row[self::NUMBER_COLUMN] ?? 0);
    }

    public function set(string $key, int|string $value): void
    {
        $row = is_int($value)
            ? [self::VALUE_COLUMN => '', self::NUMBER_COLUMN => $value]
            : [self::VALUE_COLUMN => $value, self::NUMBER_COLUMN => 0];

        $result = $this->command('set', [$this->key($key), $row]);

        if ($result === false) {
            throw new AdapterException(sprintf('Failed to set Swoole table key "%s".', $key));
        }
    }

    public function increment(string $key, int $by = 1): int
    {
        $value = $this->command('incr', [$this->key($key), self::NUMBER_COLUMN, $by]);

        if ($value === false || $value === null) {
            throw new AdapterException(sprintf('Failed to increment Swoole table key "%s".', $key));
        }

        return (int) $value;
    }

    public function delete(string $key): void
    {
        $this->command('del', [$this->key($key)]);
    }

    /**
     * @param array<int, mixed> $arguments
     */
    private function command(string $method, array $arguments): mixed
    {
        try {
            return $this->table->{$method}(...$arguments);
        } catch (\Throwable $exception) {
            throw new AdapterException($exception->getMessage(), (int) $exception->getCode(), $exception);
        }
    }

    private function key(string $key): string
    {
        $tableKey = $this->prefix . $key;

        if (strlen($tableKey) <= self::MAX_TABLE_KEY_LENGTH) {
            return $tableKey;
        }

        return substr($this->prefix, 0, 20) . sha1($tableKey);
    }
}
