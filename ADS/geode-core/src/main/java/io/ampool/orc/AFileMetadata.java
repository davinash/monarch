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

import org.apache.orc.CompressionKind;
import org.apache.orc.FileMetaInfo;
import org.apache.orc.FileMetadata;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;

import java.io.IOException;
import java.util.List;

public class AFileMetadata implements FileMetadata {
  private final boolean isOriginalFormat = false;
  private final List<StripeInformation> stripes;
  private final CompressionKind compressionKind;
  private final int compressionBufferSize;
  private final int rowIndexStride;
  private final Object fileKey = null;
  private final List<Integer> versionList;
  private final int metadataSize;
  private final int writerVersionNum;
  private final List<OrcProto.Type> types;
  private final List<OrcProto.StripeStatistics> stripeStats;
  private final long contentLength;
  private final long numberOfRows;
  private final List<OrcProto.ColumnStatistics> fileStats;

  AFileMetadata(final FileMetaInfo fileMetaInfo) throws IOException {
    AReaderImpl.MetaInfoObjExtractor rInfo =
        new AReaderImpl.MetaInfoObjExtractor(fileMetaInfo.compressionType, fileMetaInfo.bufferSize,
            fileMetaInfo.metadataSize, fileMetaInfo.footerBuffer);
    this.compressionKind = rInfo.compressionKind;
    this.compressionBufferSize = rInfo.bufferSize;
    this.metadataSize = rInfo.metadataSize;
    this.stripeStats = rInfo.metadata.getStripeStatsList();
    this.types = rInfo.footer.getTypesList();
    this.rowIndexStride = rInfo.footer.getRowIndexStride();
    this.contentLength = rInfo.footer.getContentLength();
    this.numberOfRows = rInfo.footer.getNumberOfRows();
    this.fileStats = rInfo.footer.getStatisticsList();
    this.versionList = fileMetaInfo.versionList;
    this.writerVersionNum = fileMetaInfo.getWriterVersion().ordinal();
    this.stripes = AReaderImpl.convertProtoStripesToStripes(rInfo.footer.getStripesList());
  }

  @Override
  public boolean isOriginalFormat() {
    return isOriginalFormat;
  }

  @Override
  public List<StripeInformation> getStripes() {
    return stripes;
  }

  @Override
  public CompressionKind getCompressionKind() {
    return compressionKind;
  }

  @Override
  public int getCompressionBufferSize() {
    return compressionBufferSize;
  }

  @Override
  public int getRowIndexStride() {
    return rowIndexStride;
  }

  @Override
  public int getColumnCount() {
    return 0;
  }

  @Override
  public int getFlattenedColumnCount() {
    return 0;
  }

  @Override
  public Object getFileKey() {
    return fileKey;
  }

  @Override
  public List<Integer> getVersionList() {
    return versionList;
  }

  @Override
  public int getMetadataSize() {
    return metadataSize;
  }

  @Override
  public int getWriterVersionNum() {
    return writerVersionNum;
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return types;
  }

  @Override
  public List<OrcProto.StripeStatistics> getStripeStats() {
    return stripeStats;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public long getNumberOfRows() {
    return numberOfRows;
  }

  @Override
  public List<OrcProto.ColumnStatistics> getFileStats() {
    return fileStats;
  }
}
