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

package io.ampool

import java.util

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
  * The pre-serialization and post-deserialization converters
  * for the respective types (from Java) to convert the respective
  * data-types to/from Scala. These converters simply convert the
  * Java/Scala data-types (like array/map) from one type to other.
  *
  * These are executed before serialization or after deserialization.
  *
  * Created on: 2016-05-05
  * Since version: 0.3.3.0
  */
object DataConverter extends Serializable {
  def arrToAmpool(in: Any): Array[Any] = {
    in.asInstanceOf[Seq[Any]].toArray
  }
  def arrToSpark(in: Any): Seq[Any] = {
    in.asInstanceOf[java.util.List[Any]].toArray
  }
  def mapToAmpool(in: Any): util.Map[Any, Any] = {
    mapAsJavaMap(in.asInstanceOf[Map[Any,Any]])
  }
  def mapToSpark(in: Any): Map[Any, Any] = {
    in.asInstanceOf[java.util.Map[Any,Any]].toMap
  }
  def stToAmpool(in: Any): Array[Any] = {
    arrToAmpool(in.asInstanceOf[Row].toSeq)
  }
  def stToSpark(in: Any): Row = {
    Row.fromSeq(in.asInstanceOf[Array[Any]])
  }

  /**
    * Convert the from struct format to the respective Vector (sparse/dense).
    *
    * @param in the struct-object to be converted to vector
    * @return the respective vector
    */
  def toVector(in: Any): Vector = {
    val list = in.asInstanceOf[Array[Any]]
    list(0).asInstanceOf[Byte] match {
      case 1 =>
        Vectors.dense(list(3).asInstanceOf[java.util.List[Double]].map(_.toDouble).toArray)
      case 0 =>
        Vectors.sparse(list(1).asInstanceOf[Int],
          list(2).asInstanceOf[java.util.List[Int]].map(_.toInt).toArray,
          list(3).asInstanceOf[java.util.List[Double]].map(_.toDouble).toArray)
    }
  }

  /**
    * Convert the Vector (sparse/dense) to struct (an array of values) with respective type.
    *
    * @param in the input vector
    * @return an array of the required values in struct-like format
    */
  def fromVector(in: Any): Array[Any] = {
    in match {
      case SparseVector(size, indices, values) =>
        Array(0.asInstanceOf[Byte], size,
          indices.asInstanceOf[Array[Int]],
          values.asInstanceOf[Array[Double]])
      case DenseVector(values) =>
        Array(1.asInstanceOf[Byte], null, null, values.asInstanceOf[Array[Double]])
    }
  }

  def convertWrite(dt: DataType, value: Any): Any = {
    if (value == null) {
      return null
    }
    dt match {
      case e: ArrayType =>
        value.asInstanceOf[Seq[Any]].map(v => convertWrite(dt.asInstanceOf[ArrayType]
          .elementType, v)).toArray
      case e: MapType =>
        val mdt = dt.asInstanceOf[MapType]
        mapAsJavaMap(value.asInstanceOf[Map[Any, Any]] map {
          case (k, v) => convertWrite(mdt.keyType, k) -> convertWrite(mdt.valueType, v)
        })
      case e: StructType =>
        val sdt = dt.asInstanceOf[StructType]
        value.asInstanceOf[Row].toSeq.zipWithIndex.map {
          case (cv, i) => convertWrite(sdt.get(i).dataType, cv)
        }.toArray
      case e: VectorUDT => fromVector(value)
      case _: Any => value
    }
  }

  def convertRead(dt: DataType, value: Any): Any = {
    if (value == null) {
      return null
    }
    dt match {
      case e: ArrayType =>
        val elt = dt.asInstanceOf[ArrayType].elementType
        value.asInstanceOf[java.util.List[Any]].map(convertRead(elt, _)).toArray
      case e: MapType =>
        val mdt = dt.asInstanceOf[MapType]
        value.asInstanceOf[java.util.Map[Any, Any]] map {
          case (k, v) => convertRead(mdt.keyType, k) -> convertRead(mdt.valueType, v)
        }
      case e: StructType =>
        val sdt = dt.asInstanceOf[StructType]
        val values = value.asInstanceOf[Array[Any]].zipWithIndex.map {
          case (cv, i) => convertRead(sdt.get(i).dataType, cv)
        }
        Row.fromSeq(values)
      case e: VectorUDT => toVector(value)
      case _: Any => value
    }
  }
}
