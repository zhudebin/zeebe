package io.zeebe.engine.state;

import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.impl.DbString;
import java.util.function.Supplier;

public class ZeebeColumn<K extends DbKey, V extends DbValue> {

  public static final ZeebeColumn<DbString, LastProcessedPosition> DEFAULT =
      new ZeebeColumn(ZbColumnFamilies.DEFAULT, DbString::new, LastProcessedPosition::new);

  private final ZbColumnFamilies columnFamily;

  private final Supplier<K> keyInstanceSupplier;
  private final Supplier<V> valueInstanceSupllier;

  private ZeebeColumn(
      final ZbColumnFamilies columnFamily,
      final Supplier<K> keyInstanceSupplier,
      final Supplier<V> valueInstanceSupllier) {
    this.columnFamily = columnFamily;
    this.keyInstanceSupplier = keyInstanceSupplier;
    this.valueInstanceSupllier = valueInstanceSupllier;
  }

  public ZbColumnFamilies getColumnFamily() {
    return columnFamily;
  }

  public K createKeyInstance() {
    return keyInstanceSupplier.get();
  }

  public V createValueInstance() {
    return valueInstanceSupllier.get();
  }
}
