# OBKV HBase Client
OBKV HBase Client is Java Library that can be used to access data from [OceanBase](https://github.com/oceanbase/oceanbase) by [HBase-0.94 API](https://svn.apache.org/repos/asf/hbase/hbase.apache.org/trunk/0.94/apidocs/index.html).

## Quick start

Create table in the OceanBase database:

``` sql
CREATE TABLE `test1$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);
```
**Note:**
* test1: HBase table name;
* family1: HBase column family name.

Import the dependency for your maven project:
``` xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>obkv-hbase-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

The code demo:
``` java
    // 1. initial client for table test1
    Configuration conf = new Configuration();
    conf.set(HBASE_OCEANBASE_PARAM_URL, "PARAM_URL");
    conf.set(HBASE_OCEANBASE_FULL_USER_NAME, "FULL_USER_NAME");
    conf.set(HBASE_OCEANBASE_PASSWORD, "PASSWORD");
    conf.set(HBASE_OCEANBASE_SYS_USER_NAME, "SYS_USER_NAME");
    conf.set(HBASE_OCEANBASE_SYS_PASSWORD, "SYS_PASSWORD");
    OHTableClient hTable = new OHTableClient("test1", conf);
    hTable.init();
    
    // 2. put data like hbase
    byte[] family = toBytes("family1");
    byte[] rowKey = toBytes("rowKey1");
    byte[] column = toBytes("column1");
    Put put = new Put(rowKey);
    put.add(family, column, System.currentTimeMillis(), toBytes("value1"));
    hTable.put(put);
    
    // 3. get data like hbase
    Get get = new Get(rowKey);
    get.addColumn(family, column);
    Result r = hTable.get(get);
    System.out.printf("column1: " + r.getColumn(family, column));
    
    // ...
```
**NOTE:**
* param_url is generated by ConfigServer (link TODO). 
* More example [TODO]
## Documentation

- English [Coming soon]
- Simplified Chinese (简体中文) [Coming soon]

## Licencing

OBKV HBase Client is under [MulanPSL - 2.0](http://license.coscl.org.cn/MulanPSL2) licence. You can freely copy and use the source code. When you modify or distribute the source code, please obey the MulanPSL - 2.0 licence.

## Contributing

Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

- Raise us an [Issue](https://github.com/oceanbase/obkv-hbase-client-java/issues)
- Submit Pull Requests. For details, see [How to contribute](CONTRIBUTING.md).

## Support

In case you have any problems when using OceanBase Database, welcome reach out for help:

- GitHub Issue [GitHub Issue](https://github.com/oceanbase/obkv-hbase-client-java/issues)
- Official forum [Official website](https://open.oceanbase.com)
- Knowledge base [Coming soon]
