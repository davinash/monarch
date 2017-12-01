/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package io.ampool.monarch.table.coprocessor.internal;

import io.ampool.monarch.table.coprocessor.MResultSender;
import org.apache.geode.cache.execute.ResultSender;

/**
 * MResultSenderImpl
 *
 * @since 0.2.0.0
 */

public class MResultSenderImpl implements MResultSender {

  private ResultSender resultSender;

  public MResultSenderImpl(ResultSender resultSender) {
    this.resultSender = resultSender;
  }

  public ResultSender getResultSender() {
    return resultSender;
  }

  @Override
  public void sendResult(Object oneResult) {
    resultSender.sendResult(oneResult);
  }

  @Override
  public void lastResult(Object lastResult) {
    resultSender.lastResult(lastResult);
  }

  @Override
  public void sendException(Throwable throwable) {
    resultSender.sendException(throwable);
  }
}
