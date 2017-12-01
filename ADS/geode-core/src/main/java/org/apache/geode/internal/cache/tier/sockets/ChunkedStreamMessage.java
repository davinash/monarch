/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache.tier.sockets;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.geode.SerializationException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.MessageStats;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.util.BlobHelper;

/**
 * Created by rgeiger on 5/29/16.
 *
 * Ampool addition to Geode classes to support faster streaming data transfer functionality;
 * licensed to Apache Geode for possible contribution.
 */
public class ChunkedStreamMessage extends ChunkedMessage {
  // cache HeapDataOutputStream objects for re-use
  HeapDataOutputStream[] hdosList = null;
  int hdosListSize = 0;

  public ChunkedStreamMessage(int numberOfParts, Version version) {
    super(numberOfParts, version);
    hdosList = new HeapDataOutputStream[10];
    hdosListSize = 0;
  }

  /**
   * Utility method to allow the HeapDataOutputStream cache to be cleaned of elements (reduce the
   * number of elements in the cache). Does not affect the cache capacity.
   *
   * @param num the size to trim to.
   */
  public void trimOutputStreamCache(int num) {
    if (num >= 0 && hdosList != null) {
      synchronized (hdosList) {
        while (hdosListSize > num) {
          hdosList[hdosListSize - 1] = null;
          hdosListSize -= 1;
        }
      }
    }
  }

  // Change below is the re-use of cached HeapDataOutputStream objects
  @Override
  protected void serializeAndAddPartNoCopying(Object o) {
    HeapDataOutputStream hdos = null;
    Version v = version;

    if (version.equals(Version.CURRENT)) {
      v = null;
    }

    synchronized (hdosList) {
      if (hdosListSize != 0) {
        hdos = hdosList[hdosListSize - 1];
        hdos.reset();
        hdosListSize -= 1;
      }
    }
    if (hdos == null) {
      // create the HDOS with a flag telling it that it can keep any byte[] or
      // ByteBuffers/ByteSources passed to it.
      hdos = new HeapDataOutputStream(getChunkSize(), v, true);
    }

    try {
      BlobHelper.serializeTo(o, hdos);
    } catch (IOException ex) {
      throw new SerializationException("failed serializing object", ex);
    }
    this.messageModified = true;
    Part part = partsList[this.currentPart];
    part.setPartState(hdos, true);
    this.currentPart++;

    synchronized (hdosList) {
      hdosList[hdosListSize] = hdos;
      hdosListSize++;
    }
  }

  // Change below is to support HeapDataOutputStream object recycling
  @Override
  public void sendChunk() throws IOException {
    if (isLastChunk()) {
      this.headerSent = false;
    }

    sendBytes(false);
    clearPartsAndrecycleOutputStreams();
    // release the HeapDataOutputStream objects from the cache
    if (isLastChunk()) {
      trimOutputStreamCache(0);
    }
  }

  public void clearPartsAndrecycleOutputStreams() {
    synchronized (hdosList) {
      for (int i = 0; i < numberOfParts; i++) {
        if (hdosListSize == hdosList.length) {
          break;
        }
        HeapDataOutputStream hdos = partsList[i].getOutputStream();
        if (hdos != null) {
          hdos.reset();
          hdosList[hdosListSize++] = hdos;
        }
      }
    }
    for (Part p : partsList) {
      p.clear();
    }
    this.currentPart = 0;
  }

  // Change below is the use of BufferedOutputStream
  @Override
  public void setComms(Socket socket, InputStream is, OutputStream os, ByteBuffer bb,
      MessageStats msgStats) throws IOException {
    Assert.assertTrue(socket != null);
    this.socket = socket;
    this.sockCh = socket.getChannel();
    this.is = is;
    this.os = new BufferedOutputStream(os);
    this.cachedCommBuffer = bb;
    this.msgStats = msgStats;
  }
}
