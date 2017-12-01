package io.ampool.monarch.table.loader;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.internal.MOperation;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.MValue;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public class MultiTableColumnLoaderException implements CacheLoader {
  protected static final Logger logger = LogService.getLogger();

  @Override
  public void close() {

  }

  @Override
  public Object load(LoaderHelper helper) throws CacheLoaderException {
    try {
      throw new CacheLoaderException();
    } catch (Exception e) {

    }
    return null;
  }
}
