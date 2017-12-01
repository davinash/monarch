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

package io.ampool.tierstore.config.parquet;

/**
 * Parquet Writer config in addition to CommonConfig
 */
public class ParquetWriterConfig {

  /**
   * Valid values are UNCOMPRESSED - No compression SNAPPY -
   * org.apache.parquet.hadoop.codec.SnappyCodec GZIP - org.apache.hadoop.io.compress.GzipCodec LZO
   * - com.hadoop.compression.lzo.LzoCodec
   */
  public static final String COMPRESSION_CODEC_NAME = "compression.codec.name";
  public static final String BLOCK_SIZE = "block.size";
  public static final String PAGE_SIZE = "page.size";
}
