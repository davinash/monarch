/*
 * ========================================================================= Copyright (c) 2015
 * Ampool, Inc. All Rights Reserved. This product is protected by U.S. and international copyright
 * and intellectual property laws.
 * =========================================================================
 */

package io.ampool.monarch.table;


import java.io.Serializable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum MDiskWritePolicy implements Serializable {
  SYNCHRONOUS, ASYNCHRONOUS
}
