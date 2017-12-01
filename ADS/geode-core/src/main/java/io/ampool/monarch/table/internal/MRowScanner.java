package io.ampool.monarch.table.internal;

import io.ampool.monarch.table.Row;

import java.io.Closeable;

public interface MRowScanner extends Closeable, Iterable<Row> {
}
