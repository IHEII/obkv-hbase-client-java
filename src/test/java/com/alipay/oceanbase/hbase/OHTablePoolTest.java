/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
 * %%
 * OBKV HBase Client Framework  is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.hbase;

import com.alipay.remoting.util.ConcurrentHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.PoolMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.hbase.util.ObTableClientManager.OB_TABLE_CLIENT_INSTANCE;
import static com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets.table;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class OHTablePoolTest extends HTableTestBase {
    protected OHTablePool ohTablePool;

    private OHTablePool newOHTablePool(final int maxSize, final PoolMap.PoolType poolType) {
        OHTablePool pool = new OHTablePool(new Configuration(), maxSize, poolType);
        pool.setFullUserName("test", ObHTableTestUtil.FULL_USER_NAME);
        pool.setPassword("test", ObHTableTestUtil.PASSWORD);
        if (ObHTableTestUtil.ODP_MODE) {
            pool.setOdpAddr("test", ObHTableTestUtil.ODP_ADDR);
            pool.setOdpPort("test", ObHTableTestUtil.ODP_PORT);
            pool.setOdpMode("test", ObHTableTestUtil.ODP_MODE);
            pool.setDatabase("test", ObHTableTestUtil.DATABASE);
        } else {
            pool.setParamUrl("test", ObHTableTestUtil.PARAM_URL);
            pool.setSysUserName("test", ObHTableTestUtil.SYS_USER_NAME);
            pool.setSysPassword("test", ObHTableTestUtil.SYS_PASSWORD);
        }
        return pool;
    }

    @Before
    public void setup() throws IOException {
        Configuration c = new Configuration();
        ohTablePool = newOHTablePool(10, null);
        ohTablePool.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(3));
        hTable = ohTablePool.getTable("test");
    }

    @After
    public void finish() throws IOException {
        hTable.close();
    }

    public void test_current_get_close(final OHTablePool ohTablePool, int concurrency, int maxSize) {
        final CountDownLatch pre = new CountDownLatch(concurrency);
        final CountDownLatch suf = new CountDownLatch(concurrency);
        final ConcurrentHashSet<HTableInterface> ohTableSet = new ConcurrentHashSet<HTableInterface>();
        final ConcurrentHashSet<HTableInterface> pooledHTableSet = new ConcurrentHashSet<HTableInterface>();
        for (int i = 0; i < concurrency; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    pre.countDown();
                    try {
                        pre.await();
                    } catch (InterruptedException e) {
                        //
                    }
                    OHTablePool.PooledOHTable pooledOHTable = ((OHTablePool.PooledOHTable) ohTablePool
                        .getTable("test"));
                    HTableInterface htable = pooledOHTable.getTable();
                    ohTableSet.add(htable);
                    pooledHTableSet.add(pooledOHTable);
                    suf.countDown();
                }
            }).start();
        }

        try {
            suf.await();
        } catch (InterruptedException e) {
            //
        }

        for (HTableInterface htable : pooledHTableSet) {
            try {
                htable.close();
            } catch (IOException e) {
                Assert.fail();
            }
        }
        Assert.assertEquals(concurrency, ohTableSet.size());
        Assert.assertEquals(1, OB_TABLE_CLIENT_INSTANCE.size());
        Assert.assertEquals(maxSize, ohTablePool.getCurrentPoolSize("test"));
    }

    @Test
    public void test_refresh_table_entry() throws Exception {
        ohTablePool.refreshTableEntry("test", "family1", false);
        ohTablePool.refreshTableEntry("test", "family1", true);
    }

    @Test
    public void test_all_type_pool() throws Exception {
        OHTablePool ohTablePool = newOHTablePool(10, PoolMap.PoolType.Reusable);

        // test first
        test_current_get_close(ohTablePool, 1000, 10);
        // test reuse
        test_current_get_close(ohTablePool, 1000, 10);
        ohTablePool.close();

        ohTablePool = newOHTablePool(10, PoolMap.PoolType.ThreadLocal);
        //ohTablePool.load("test", "oceanbase_stable_test_host");

        // test first
        test_current_get_close(ohTablePool, 1000, 1);
        // test reuse
        test_current_get_close(ohTablePool, 1000, 1);
        ohTablePool.close();

        ohTablePool = newOHTablePool(10, PoolMap.PoolType.RoundRobin);
        //ohTablePool.load("test", "oceanbase_stable_test_host");

        // test first
        test_current_get_close(ohTablePool, 1000, 10);
        // test reuse
        test_current_get_close(ohTablePool, 1000, 10);
        ohTablePool.close();
    }

    class ScanTask implements Runnable {
        private final OHTablePool hTablePool;
        private final String tableName;
        private final byte[] family;
        private final String startKey;
        private final String stopKey;
        private final int iterTimes;

        public ScanTask(OHTablePool hTablePool, String tableName, byte[] family, String startKey, String stopKey, int iterTimes) {
            this.hTablePool = hTablePool;
            this.tableName = tableName;
            this.family = family;
            this.startKey = startKey;
            this.stopKey = stopKey;
            this.iterTimes = iterTimes;
        }

        @Override
        public void run() {
            HTableInterface hTable = null;
            for (int i = 0; i < iterTimes; i++) {
                if (i > 0 && i % 1000 == 0) {
                    System.out.println(this + " process: " +  i);
                }
                try {
                    hTable = hTablePool.getTable(tableName);
                    Scan scan = new Scan();
                    scan.addFamily(family);
                    scan.setStartRow(startKey.getBytes());
                    scan.setStopRow(stopKey.getBytes());
                    scan.setMaxVersions(10);
                    int resCount = 0;
                    try (ResultScanner scanner = hTable.getScanner(scan)) {
                        for (Result result : scanner) {
                            for (KeyValue keyValue : result.raw()) {
                                resCount += 1;
                            }
                        }
                        Assert.assertEquals(13, resCount);
                        // System.out.println(this + " result count: " +  resCount);
                    } catch (Exception e) {
                        System.err.println("Error occurred while scanning: " + e.getMessage());
                    }
                } catch (Exception e) {
                    System.err.println("Thread error: " + e.getMessage());
                } finally {
                    if (hTable != null) {
                        try {
                            hTable.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testMSScan() throws Exception {
        String key1 = "scanKey1x";
        String key2 = "scanKey2x";
        String key3 = "scanKey3x";
        String zKey1 = "zScanKey1";
        String zKey2 = "zScanKey2";
        String column1 = "column1";
        String column2 = "column2";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String family = "family1";

        // delete previous data
        Delete deleteKey1Family = new Delete(toBytes(key1));
        deleteKey1Family.deleteFamily(toBytes(family));
        Delete deleteKey2Family = new Delete(toBytes(key2));
        deleteKey2Family.deleteFamily(toBytes(family));
        Delete deleteKey3Family = new Delete(toBytes(key3));
        deleteKey3Family.deleteFamily(toBytes(family));
        Delete deleteZKey1Family = new Delete(toBytes(zKey1));
        deleteZKey1Family.deleteFamily(toBytes(family));
        Delete deleteZKey2Family = new Delete(toBytes(zKey2));
        deleteZKey2Family.deleteFamily(toBytes(family));

        hTable.delete(deleteKey1Family);
        hTable.delete(deleteKey2Family);
        hTable.delete(deleteKey3Family);
        hTable.delete(deleteZKey1Family);
        hTable.delete(deleteZKey2Family);

        Put putKey1Column1Value1 = new Put(toBytes(key1));
        putKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey1Column1Value2 = new Put(toBytes(key1));
        putKey1Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey1Column2Value2 = new Put(toBytes(key1));
        putKey1Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey1Column2Value1 = new Put(toBytes(key1));
        putKey1Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey2Column1Value1 = new Put(toBytes(key2));
        putKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey2Column1Value2 = new Put(toBytes(key2));
        putKey2Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey2Column2Value2 = new Put(toBytes(key2));
        putKey2Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putKey2Column2Value1 = new Put(toBytes(key2));
        putKey2Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column1Value1 = new Put(toBytes(key3));
        putKey3Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putKey3Column1Value2 = new Put(toBytes(key3));
        putKey3Column1Value2.add(toBytes(family), toBytes(column1), toBytes(value2));

        Put putKey3Column2Value1 = new Put(toBytes(key3));
        putKey3Column2Value1.add(toBytes(family), toBytes(column2), toBytes(value1));

        Put putKey3Column2Value2 = new Put(toBytes(key3));
        putKey3Column2Value2.add(toBytes(family), toBytes(column2), toBytes(value2));

        Put putzKey1Column1Value1 = new Put(toBytes(zKey1));
        putzKey1Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Put putzKey2Column1Value1 = new Put(toBytes(zKey2));
        putzKey2Column1Value1.add(toBytes(family), toBytes(column1), toBytes(value1));

        Get get;
        Scan scan;
        Result r;
        int res_count = 0;

        tryPut(hTable, putKey1Column1Value1);
        tryPut(hTable, putKey1Column1Value2);
        tryPut(hTable, putKey1Column1Value1); // 2 * putKey1Column1Value1
        tryPut(hTable, putKey1Column2Value1);
        tryPut(hTable, putKey1Column2Value2);
        tryPut(hTable, putKey1Column2Value1); // 2 * putKey1Column2Value1
        tryPut(hTable, putKey1Column2Value2); // 2 * putKey1Column2Value2
        tryPut(hTable, putKey2Column2Value1);
        tryPut(hTable, putKey2Column2Value2);
        tryPut(hTable, putKey3Column1Value1);
        tryPut(hTable, putKey3Column1Value2);
        tryPut(hTable, putKey3Column2Value1);
        tryPut(hTable, putKey3Column2Value2);
        tryPut(hTable, putzKey1Column1Value1);
        tryPut(hTable, putzKey2Column1Value1);

        // show table (time maybe different)
        //+-----------+---------+----------------+--------+
        //| K         | Q       | T              | V      |
        //+-----------+---------+----------------+--------+
        //| scanKey1x | column1 | -1709714409669 | value1 |
        //| scanKey1x | column1 | -1709714409637 | value2 |
        //| scanKey1x | column1 | -1709714409603 | value1 |
        //| scanKey1x | column2 | -1709714409802 | value2 |
        //| scanKey1x | column2 | -1709714409768 | value1 |
        //| scanKey1x | column2 | -1709714409735 | value2 |
        //| scanKey1x | column2 | -1709714409702 | value1 |
        //| scanKey2x | column2 | -1709714409869 | value2 |
        //| scanKey2x | column2 | -1709714409836 | value1 |
        //| scanKey3x | column1 | -1709714409940 | value2 |
        //| scanKey3x | column1 | -1709714409904 | value1 |
        //| scanKey3x | column2 | -1709714410010 | value2 |
        //| scanKey3x | column2 | -1709714409977 | value1 |
        //| zScanKey1 | column1 | -1713778598591 | value1 |
        //| zScanKey2 | column1 | -1713778598625 | value1 |
        //+-----------+---------+----------------+--------+

        // scan with prefixFilter
        ExecutorService executorService = Executors.newFixedThreadPool(5); // 假设我们想要同时运行 5 个扫描任务

        // 创建并启动线程
        for (int i = 0; i < 5; i++) {
            executorService.submit(new ScanTask(ohTablePool, "test", toBytes(family), "scanKey1x", "scanKey4x", 100000));
        }

        // 当所有任务完成后关闭线程池
        executorService.shutdown();

        // 等待所有已提交的任务完成，或者最长等待一定时间
        try {
            // 设置最大等待时间，比如60秒
            if (!executorService.awaitTermination(600, TimeUnit.SECONDS)) {
                // 超时后尝试停止所有正在执行的任务
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            // 当前线程被中断时（比如在调用awaitTermination时）
            executorService.shutdownNow();
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
    }

}
