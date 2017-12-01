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

// package io.ampool.monarch.table.facttable.junit;
//
// import static org.junit.Assert.*;
//
// import io.ampool.monarch.table.Bytes;
// import io.ampool.monarch.table.MScan;
// import io.ampool.monarch.table.filter.KeyOnlyFilter;
// import io.ampool.monarch.table.filter.MFilter;
// import io.ampool.monarch.table.filter.MFilterList;
// import io.ampool.monarch.table.filter.MFilterList.Operator;
// import io.ampool.monarch.table.filter.RowFilter;
// import io.ampool.monarch.table.filter.SingleColumnValueFilter;
// import io.ampool.monarch.table.ftable.FTableDescriptor;
// import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
// import io.ampool.monarch.types.CompareOp;
// import org.junit.After;
// import org.junit.Before;
// import org.junit.Test;
// import org.junit.experimental.categories.Category;
//
// import org.apache.geode.test.junit.categories.FTableTest;
//
//

// @Category(FTableTest.class)
// public class ConvertFilterTest {
//
// @Before
// public void setUp() throws Exception {
//
// }
//
// @After
// public void tearDown() throws Exception {
//
// }
//
// /**
// * Test convert MFilterList filters to RowKeyFilter
// * @throws Exception
// */
// @Test
// public void convertMFilterList() throws Exception {
// MFilter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
// CompareOp.GREATER, 1000l);
// MFilter filter2 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
// CompareOp.LESS, 1500l);
//
// MFilterList filterList = new MFilterList(Operator.MUST_PASS_ALL,filter1,filter2);
// MScan scan = new MScan();
// scan.setFilter(filterList);
//
// ProxyFTableRegion impl = new ProxyFTableRegion();
// final MScan iScan = impl.handleSpecialColumnFilters(scan);
// assertTrue(scan.hasFilter() == iScan.hasFilter());
// assertTrue(iScan.getFilter() instanceof MFilterList);
// final MFilterList filterListNew = (MFilterList) iScan.getFilter();
// assertTrue(filterList.getOperator().equals(filterListNew.getOperator()));
// assertEquals(filterList.isReversed(),filterListNew.isReversed());
//
// for (int i = 0; i < filterListNew.getFilters().size(); i++) {
// final MFilter filter = filterListNew.getFilters().get(i);
// assertTrue(filter instanceof RowFilter);
// final RowFilter rFilter = (RowFilter) filter;
// if(i == 0){
// assertEquals(CompareOp.GREATER,rFilter.getOperator());
// assertEquals(Bytes.compareTo(Bytes.toBytes(1000l),(byte[])rFilter.getValue()),0);
// }else{
// assertEquals(CompareOp.LESS,rFilter.getOperator());
// assertEquals(Bytes.compareTo(Bytes.toBytes(1500l),(byte[])rFilter.getValue()),0);
// }
// }
//
// }
//
// /**
// * Test convert MFilterList filters to RowKeyFilter
// * @throws Exception
// */
// @Test
// public void convertMFilterListWithDiffColName() throws Exception {
// MFilter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
// CompareOp.GREATER, 1000l);
// MFilter filter2 = new SingleColumnValueFilter("COL1", CompareOp.LESS, 1500l);
//
// MFilterList filterList = new MFilterList(Operator.MUST_PASS_ALL,filter1,filter2);
// MScan scan = new MScan();
// scan.setFilter(filterList);
//
// ProxyFTableRegion impl = new ProxyFTableRegion();
// final MScan iScan = impl.handleSpecialColumnFilters(scan);
// assertTrue(scan.hasFilter() == iScan.hasFilter());
// assertTrue(iScan.getFilter() instanceof MFilterList);
// final MFilterList filterListNew = (MFilterList) iScan.getFilter();
// assertTrue(filterList.getOperator().equals(filterListNew.getOperator()));
// assertEquals(filterList.isReversed(),filterListNew.isReversed());
//
// for (int i = 0; i < filterListNew.getFilters().size(); i++) {
// final MFilter filter = filterListNew.getFilters().get(i);
// if(i == 0){
// assertTrue(filter instanceof RowFilter);
// final RowFilter rFilter = (RowFilter) filter;
// assertEquals(CompareOp.GREATER,rFilter.getOperator());
// assertEquals(Bytes.compareTo(Bytes.toBytes(1000l),(byte[])rFilter.getValue()),0);
// }else{
// assertTrue(filter instanceof SingleColumnValueFilter);
// final SingleColumnValueFilter rFilter = (SingleColumnValueFilter) filter;
// assertEquals(CompareOp.LESS,rFilter.getOperator());
// assertEquals(new Long(1500l),(Long)rFilter.getValue());
// }
// }
//
// }
//
// /**
// * Test convert SingleColumnValueFilter to RowKeyFilter
// * @throws Exception
// */
// @Test
// public void convertSingleColumnValueFilter() throws Exception {
// MFilter filter1 = new SingleColumnValueFilter(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
// CompareOp.GREATER, 1000l);
//
// MScan scan = new MScan();
// scan.setFilter(filter1);
//
// ProxyFTableRegion impl = new ProxyFTableRegion();
// final MScan iScan = impl.handleSpecialColumnFilters(scan);
//
// assertTrue(scan.hasFilter() == iScan.hasFilter());
// assertTrue(iScan.getFilter() instanceof RowFilter);
// final RowFilter rFilter = (RowFilter) iScan.getFilter();
// assertEquals(CompareOp.GREATER,rFilter.getOperator());
// assertEquals(Bytes.compareTo(Bytes.toBytes(1000l),(byte[])rFilter.getValue()),0);
// }
//
// /**
// * Test convert SingleColumnValueFilter with column name other than
// * Insertion timestamp.
// * This should not convert any filter
// * @throws Exception
// */
// @Test
// public void convertSingleColumnValueFilterWithDiffName() throws Exception {
// MFilter filter1 = new SingleColumnValueFilter("COL1", CompareOp.GREATER, 1000l);
//
// MScan scan = new MScan();
// scan.setFilter(filter1);
//
// ProxyFTableRegion impl = new ProxyFTableRegion();
// final MScan iScan = impl.handleSpecialColumnFilters(scan);
//
// assertTrue(scan.hasFilter() == iScan.hasFilter());
// assertTrue(iScan.getFilter() instanceof SingleColumnValueFilter);
//
// final SingleColumnValueFilter rFilter = (SingleColumnValueFilter) iScan.getFilter();
// assertEquals(CompareOp.GREATER,rFilter.getOperator());
// assertEquals(new Long(1000l),(Long)rFilter.getValue());
// }
//
// /**
// * Test convert KeyOnlyFilter. This should keep it as it is
// * @throws Exception
// */
// @Test
// public void convertKeyOnlyFilterFilter() throws Exception {
// MFilter filter1 = new KeyOnlyFilter();
//
// MScan scan = new MScan();
// scan.setFilter(filter1);
//
// ProxyFTableRegion impl = new ProxyFTableRegion();
// final MScan iScan = impl.handleSpecialColumnFilters(scan);
//
// assertTrue(scan.hasFilter() == iScan.hasFilter());
// assertTrue(iScan.getFilter() instanceof KeyOnlyFilter);
// }
//
// }
