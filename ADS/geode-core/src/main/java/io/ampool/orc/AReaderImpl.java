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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.ReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.orc.CompressionCodec;
import org.apache.orc.FileMetaInfo;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AReaderImpl extends org.apache.hadoop.hive.ql.io.orc.ReaderImpl {

  private static final Logger LOG = LoggerFactory.getLogger(AReaderImpl.class);

  // serialized footer - Keeping this around for use by getFileMetaInfo()
  // will help avoid cpu cycles spend in deserializing at cost of increased
  // memory footprint.
  private ByteBuffer footerByteBuffer;
  // Same for metastore cache - maintains the same background buffer, but includes postscript.
  // This will only be set if the file footer/metadata was read from disk.
  private ByteBuffer footerMetaAndPsBuffer;

  public static final Path PATH = new Path(new File(".").getAbsolutePath());
  private final byte[] bytes;

  AReaderImpl(final byte[] bytes, final OrcFile.ReaderOptions options) throws IOException {
    super(PATH, options);
    this.bytes = bytes;
    this.footerMetaAndPsBuffer = options.getFileMetaInfo().footerMetaAndPsBuffer;
  }

  public static List<StripeInformation> convertProtoStripesToStripes(
          List<OrcProto.StripeInformation> list) {
    return ReaderImpl.convertProtoStripesToStripes(list);
  }

  private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos, int footerSize,
                                               CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(footerAbsPos);
    bb.limit(footerAbsPos + footerSize);
    return OrcProto.Footer.parseFrom(InStream.createCodedInputStream("footer",
            Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), footerSize, codec, bufferSize));
  }

  private static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
                                                   int metadataSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(metadataAbsPos);
    bb.limit(metadataAbsPos + metadataSize);
    return OrcProto.Metadata.parseFrom(InStream.createCodedInputStream("metadata",
            Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), metadataSize, codec, bufferSize));
  }

  private static OrcProto.PostScript extractPostScript(ByteBuffer bb, Path path, int psLen,
                                                       int psAbsOffset) throws IOException {
    // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
    assert bb.hasArray();
    CodedInputStream in =
            CodedInputStream.newInstance(bb.array(), bb.arrayOffset() + psAbsOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    checkOrcVersion(LOG, path, ps.getVersionList());

    // Check compression codec.
    switch (ps.getCompression()) {
      case NONE:
        break;
      case ZLIB:
        break;
      case SNAPPY:
        break;
      case LZO:
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    return ps;
  }

  static FileMetaInfo extractMetaInfoFromFooter(final byte[] bytes) throws IOException {
    // FSDataInputStream file = fs.open(path);
    ByteBuffer buffer = null, fullFooterBuffer = null;
    OrcProto.PostScript ps = null;
    OrcFile.WriterVersion writerVersion = null;
    try {
      int readSize = bytes.length;
      buffer = ByteBuffer.wrap(bytes);
      buffer.position(0);

      // read the PostScript
      // get length of PostScript
      int psLen = buffer.get(readSize - 1) & 0xff;
      // ensureOrcFooter(file, path, psLen, buffer);
      int psOffset = readSize - 1 - psLen;
      ps = extractPostScript(buffer, PATH, psLen, psOffset);

      int footerSize = (int) ps.getFooterLength();
      int metadataSize = (int) ps.getMetadataLength();
      writerVersion = extractWriterVersion(ps);

      // check if extra bytes need to be read
      int extra = Math.max(0, psLen + 1 + footerSize + metadataSize - readSize);
      if (extra > 0) {
        // more bytes need to be read, seek back to the right place and read extra bytes
        ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
        extraBuf.position(extra);
        // append with already read bytes
        extraBuf.put(buffer);
        buffer = extraBuf;
        buffer.position(0);
        fullFooterBuffer = buffer.slice();
        buffer.limit(footerSize + metadataSize);
      } else {
        // footer is already in the bytes in buffer, just adjust position, length
        buffer.position(psOffset - footerSize - metadataSize);
        fullFooterBuffer = buffer.slice();
        buffer.limit(psOffset);
      }

      // remember position for later TODO: what later? this comment is useless
      buffer.mark();
    } finally {
      try {
        // file.close();
      } catch (Exception ex) {
        LOG.error("Failed to close the file after another error", ex);
      }
    }

    return new FileMetaInfo(ps.getCompression().toString(), (int) ps.getCompressionBlockSize(),
            (int) ps.getMetadataLength(), buffer, ps.getVersionList(), writerVersion, fullFooterBuffer);
  }

  /**
   * MetaInfoObjExtractor - has logic to create the values for the fields in ReaderImpl from
   * serialized fields. As the fields are final, the fields need to be initialized in the
   * constructor and can't be done in some helper function. So this helper class is used instead.
   *
   */
  static class MetaInfoObjExtractor {
    final org.apache.orc.CompressionKind compressionKind;
    final CompressionCodec codec;
    final int bufferSize;
    final int metadataSize;
    final OrcProto.Metadata metadata;
    final OrcProto.Footer footer;
    final ObjectInspector inspector;

    MetaInfoObjExtractor(String codecStr, int bufferSize, int metadataSize, ByteBuffer footerBuffer)
            throws IOException {

      this.compressionKind = org.apache.orc.CompressionKind.valueOf(codecStr.toUpperCase());
      this.bufferSize = bufferSize;
      this.codec = WriterImpl.createCodec(compressionKind);
      this.metadataSize = metadataSize;

      int position = footerBuffer.position();
      int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;

      this.metadata = extractMetadata(footerBuffer, position, metadataSize, codec, bufferSize);
      this.footer =
              extractFooter(footerBuffer, position + metadataSize, footerBufferSize, codec, bufferSize);

      footerBuffer.position(position);
      this.inspector = null;
    }
  }

  public FileMetaInfo getFileMetaInfo() {
    return new FileMetaInfo(compressionKind.toString(), bufferSize, getMetadataSize(),
            footerByteBuffer, getVersionList(), getWriterVersion(), footerMetaAndPsBuffer);
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return footerMetaAndPsBuffer;
  }

  @Override
  public RecordReader rows() throws IOException {
    return rowsOptions(new Options());
  }

  @Override
  public RecordReader rowsOptions(Options opts) throws IOException {
    final Options options = opts.clone();
    // LOG.info("Reading ORC rows from " + path + " with " + options);
    boolean[] include = options.getInclude();
    // if included columns is null, then include all columns
    if (include == null) {
      include = new boolean[types.size()];
      Arrays.fill(include, true);
      options.include(include);
    }
    final DataReaderProperties drp = DataReaderProperties.builder().withFileSystem(this.fileSystem)
            .withPath(PATH).withBufferSize(this.bufferSize).withTypeCount(this.types.size())
            .withZeroCopy(true).withCompression(this.getCompressionKind()).build();
    options.dataReader(new ADataReader(drp, this.bytes));
    options.schema(this.getSchema());
    return new RecordReaderImplA(this, options);
  }

  public static final class RecordReaderImplA extends RecordReaderImpl {
    RecordReaderImplA(ReaderImpl fileReader, Options options) throws IOException {
      super(fileReader, options);
    }
  }

  @Override
  public String toString() {
    return "AMPOOL-ORC " + super.toString();
  }
}
