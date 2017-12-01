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
package io.ampool.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;

class ADataReader implements DataReader {
  private FSDataInputStream file = null;
  private HadoopShims.ZeroCopyReaderShim zcr = null;
  private final FileSystem fs;
  private final Path path;
  private final boolean useZeroCopy;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final int typeCount;
  private final byte[] bytes;

  private ADataReader(ADataReader other) {
    this.bytes = other.bytes;
    this.bufferSize = other.bufferSize;
    this.typeCount = other.typeCount;
    this.fs = other.fs;
    this.path = other.path;
    this.useZeroCopy = other.useZeroCopy;
    this.codec = other.codec;
  }

  ADataReader(DataReaderProperties properties, final byte[] bytes) {
    this.fs = properties.getFileSystem();
    this.path = properties.getPath();
    this.useZeroCopy = properties.getZeroCopy();
    this.codec = org.apache.orc.impl.WriterImpl.createCodec(properties.getCompression());
    this.bufferSize = properties.getBufferSize();
    this.typeCount = properties.getTypeCount();
    this.bytes = bytes;
  }

  @Override
  public void open() throws IOException {
    this.file = new FSDataInputStream(new ADataInputStream(bytes));
  }

  @Override
  public OrcIndex readRowIndex(StripeInformation stripe, OrcProto.StripeFooter footer,
      boolean[] included, OrcProto.RowIndex[] indexes, boolean[] sargColumns,
      OrcProto.BloomFilterIndex[] bloomFilterIndices) throws IOException {
    if (file == null) {
      open();
    }
    if (footer == null) {
      footer = readStripeFooter(stripe);
    }
    if (indexes == null) {
      indexes = new OrcProto.RowIndex[typeCount];
    }
    if (bloomFilterIndices == null) {
      bloomFilterIndices = new OrcProto.BloomFilterIndex[typeCount];
    }
    long offset = stripe.getOffset();
    List<OrcProto.Stream> streams = footer.getStreamsList();
    for (int i = 0; i < streams.size(); i++) {
      OrcProto.Stream stream = streams.get(i);
      OrcProto.Stream nextStream = null;
      if (i < streams.size() - 1) {
        nextStream = streams.get(i + 1);
      }
      int col = stream.getColumn();
      int len = (int) stream.getLength();
      // row index stream and bloom filter are interlaced, check if the sarg column contains bloom
      // filter and combine the io to read row index and bloom filters for that column together
      if (stream.hasKind() && (stream.getKind() == OrcProto.Stream.Kind.ROW_INDEX)) {
        boolean readBloomFilter = false;
        if (sargColumns != null && sargColumns[col]
            && nextStream.getKind() == OrcProto.Stream.Kind.BLOOM_FILTER) {
          len += nextStream.getLength();
          i += 1;
          readBloomFilter = true;
        }
        if ((included == null || included[col]) && indexes[col] == null) {
          // byte[] buffer = new byte[len];
          // file.readFully(offset, buffer, 0, buffer.length);
          ByteBuffer bb = ByteBuffer.wrap(this.bytes, (int) offset, len);
          indexes[col] = OrcProto.RowIndex.parseFrom(
              InStream.create("index", Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)),
                  stream.getLength(), codec, bufferSize));
          if (readBloomFilter) {
            bb.position((int) stream.getLength());
            bloomFilterIndices[col] =
                OrcProto.BloomFilterIndex.parseFrom(InStream.create("bloom_filter",
                    Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), nextStream.getLength(),
                    codec, bufferSize));
          }
        }
      }
      offset += len;
    }

    OrcIndex index = new OrcIndex(indexes, bloomFilterIndices);
    return index;
  }

  @Override
  public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
    if (file == null) {
      open();
    }
    long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
    int tailLength = (int) stripe.getFooterLength();

    // read the footer
    ByteBuffer tailBuf = ByteBuffer.wrap(this.bytes, (int) offset, tailLength);
    // ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
    // file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
    return OrcProto.StripeFooter.parseFrom(InStream.createCodedInputStream("footer",
        Lists.<DiskRange>newArrayList(new BufferChunk(tailBuf, 0)), tailLength, codec, bufferSize));
  }

  @Override
  public DiskRangeList readFileData(DiskRangeList range, long baseOffset, boolean doForceDirect)
      throws IOException {
    return readDiskRanges(file, zcr, baseOffset, range, doForceDirect);
  }

  static DiskRangeList readDiskRanges(FSDataInputStream file, HadoopShims.ZeroCopyReaderShim zcr,
      long base, DiskRangeList range, boolean doForceDirect) throws IOException {
    if (range == null)
      return null;
    DiskRangeList prev = range.prev;
    if (prev == null) {
      prev = new DiskRangeList.MutateHelper(range);
    }
    while (range != null) {
      if (range.hasData()) {
        range = range.next;
        continue;
      }
      int len = (int) (range.getEnd() - range.getOffset());
      long off = range.getOffset();
      ByteBuffer bb = null;
      if (file.getWrappedStream() instanceof ADataInputStream) {
        ADataInputStream ads = (ADataInputStream) file.getWrappedStream();
        bb = ByteBuffer.wrap(ads.getBuffer(), (int) (base + off), len);
      } else {
        // Don't use HDFS ByteBuffer API because it has no readFully, and is buggy and pointless.
        byte[] buffer = new byte[len];
        file.readFully((base + off), buffer, 0, buffer.length);
        if (doForceDirect) {
          bb = ByteBuffer.allocateDirect(len);
          bb.put(buffer);
          bb.position(0);
          bb.limit(len);
        } else {
          bb = ByteBuffer.wrap(buffer);
        }
      }
      range = range.replaceSelfWith(new BufferChunk(bb, range.getOffset()));
      range = range.next;
    }
    return prev.next;
  }

  @Override
  public void close() throws IOException {
    if (file != null) {
      file.close();
    }
  }

  @Override
  public boolean isTrackingDiskRanges() {
    return zcr != null;
  }

  @Override
  public void releaseBuffer(ByteBuffer buffer) {}

  @Override
  public ADataReader clone() {
    return new ADataReader(this);
  }

}
