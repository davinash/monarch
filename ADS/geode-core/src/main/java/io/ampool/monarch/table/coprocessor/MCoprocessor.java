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

package io.ampool.monarch.table.coprocessor;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorAdapter;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import org.apache.geode.GemFireException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * MCoprocessor is a user defined code that runs in server process on each bucket of MTable. It is
 * invoked with
 * {@link io.ampool.monarch.table.MTable#coprocessorService(String, String, byte[], MExecutionRequest)}
 * or
 * {@link io.ampool.monarch.table.MTable#coprocessorService(String, String, byte[], byte[], MExecutionRequest)}.
 *
 *
 * To write your own co-processor extend this class.
 *
 * <p>
 * Example for writing own co-processor
 * </p>
 * <code>
 *   public class RowCountCoprocessor extends MCoprocessor {

        // Empty constructor is mandatory
        public RowCountCoprocessor() {

        }

        // User method
        public long rowCount(MCoprocessorContext context) {
          // User implementation
        }
     }
 * </code>
 *
 * @since 0.2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MCoprocessor extends MCoprocessorAdapter {

  private static final long serialVersionUID = 301563671820877719L;

  /**
   * It is recommanded not to overide this further.
   * 
   * @param context
   */
  @InterfaceAudience.Private
  @InterfaceStability.Stable
  @Override
  public void run(MCoprocessorContext context) {
    MExecutionRequest request = context.getRequest();
    String methodName = request.getMethodName();
    try {
      Method method =
          this.getClass().getMethod(methodName, new Class[] {MCoprocessorContext.class});
      Object res = method.invoke(this, context);
      context.getResultSender().lastResult(res);
    } catch (NoSuchMethodException | GemFireException e) {
      context.getResultSender().sendException(new MCoprocessorException(e));
    } catch (InvocationTargetException e) {
      context.getResultSender().sendException(e.getTargetException());
    } catch (IllegalAccessException e) {
      context.getResultSender().sendException(new MCoprocessorException(e));
    } catch (Exception e) {
      context.getResultSender().sendException(new MCoprocessorException(e));
    }
  }
}

