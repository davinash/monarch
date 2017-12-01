package org.apache.geode.internal.cache;

import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;

public class InvalidateAuthzCacheFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -8116355361968918736L;

  public InvalidateAuthzCacheFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    try {
      SecurityManager securityManager = SecurityService.getSecurityService().getSecurityManager();
      securityManager.invalidateAuthzCache();
    } catch (Exception e) {
      context.getResultSender().lastResult(e);
    }
    context.getResultSender().lastResult("Invalidated privileges cache");
  }

  @Override
  public String getId() {
    return InvalidateAuthzCacheFunction.class.getName();
  }
}
