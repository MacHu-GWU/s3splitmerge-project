为什么要有 s3splitmerge 这个库?
------------------------------------------------------------------------------

使用 S3 作为 Data Lake 在 AWS 上已经成为了事实标准. 对于 ETL, Athena Query 而言, 单个文件大小会严重影响性能以及导致额外的工作来处理特殊情况. 如果能够把文件归一化为比较合适的大小, 那么很多事情都会方便很多, 查询性能也会提高很多.

具体来说如果是 少量大文件, 那么会有如下问题:

1. Athena 查询时必须扫描这个大文件, 不能按照 Partition 只扫描部分数据, 带来额外的花费.
2. 单个大文件不适合用轻量, 扩展方便的容器化计算来处理. 比如 Lambda, EKS, 很难保证一个计算单元能在有限的时间和内存限制下处理完.
3. 不适合并行读写.

而如果是 大量小文件, 那么也会有如下问题:

1. Athena 查询性能会下降, 因为出现了过多的 IO.
2. ETL 的处理性能会下降, 矢量化的操作每次只能应用于比较小的文件, 效率不高.
3. Catalog 管理起来不方便, 如果每个文件用 key value pair 来表示, 那么有太多 pair 要管理.

根据历史经验, 50 ~ 200 MB 都是比较合适的文件大小.


s3splitmerge 是如何解决这一问题的?
------------------------------------------------------------------------------


**首先来看 Split**

传统的方法是, 把大文件一次性读取到内存中, 然后按行遍历, 达到一定行数就将数据子集写入小文件. 如果大文件时 1GB 小文件是 100MB, 那么计算单元至少要 1GB + 100MB 的内存.

而 s3splitmerge 用了 smart_open 这个库, 能够按照行读取大文件, 不用一次性将整个大文件读取到内存中, 这个 Buffer 可以在 1MB 左右, 那么 100MB 的内存就足够了.

该方法只对能安航遍历的文件格式起作用.

**其次来看 Merge**


具体实现
------------------------------------------------------------------------------

Parquet 格式介绍:

Parquet 是 Apache 基金会的一个项目, 最早由 twitter 和 cloudrea 合作开发, 是为大数据时代开发的 列式存储 数据格式.

存储 IO 优势:

1. 数据用 binary 编码而不是 string, 速度和效率会比用 string 编码高得多. 比如 csv, json 都是用 string 编码的.
2. 同一列的数据格式相同, 所以可以用更好的压缩算法对其压缩. 使得最终体积更小. 这会比用 csv, json 等保存后用 gzip 压缩效率更高.
3. 写入时可以指定将许多 rows 打包成一个 row group 作为一个整体的 binary 块. 这样读取时就可以一次只读取一个 row group 到内存而无需读取整个文件.

查询优势:

1. 对于查询中不需要的列可以跳过, 避免 IO.
2. 每个 row group 中的 每个 column 都有 statistics 信息, 如果查询里的 ``WHERE column BETWEEN lower AND upper`` 范围超过了 row group 的范围, 则可以跳过这部分, 从而减少了所要扫描的数据量.

缺点:

1. 由于不是按行存储, 你无法用 append write 的模式在后面增加新内容.

Split Parquet
------------------------------------------------------------------------------
定义: 该任务是将单个大文件分割成多个小文件. 例如将单个 1GB 的 .parquet 文件分割成 10 个 100MB 的小文件.

方法:

1. 如果写入的时候就定义了 ``row_group_size``, 那么可以先读 metadata, 看一共有多少个 row_group, 然后用总大小一除就可以获得每个 row group 的大概大小.
    - 如果每个 row group 的大小大于 target_size.
    - 如果每个 row



- 获得这几个变量 ``before_size``, ``target_size``, ``n_row_group``, 对于每一个 ``row_group`` 有这么几个变量 ``row_group.n_rows``, ``row_group.total_compressed_size``.
    - for each


架构设计
------------------------------------------------------------------------------

**目标**

让到达 S3 Bucket 的数据文件具有标准化的体积大小. 假设我们标准化的文件大小是 X:

- Row Oriented 格式为 X = 100MB, 例如 CSV, JSON. 这是因为 Row Oriented 的文件天生不支持并行读取, 所以文件太大会让很多 Work Node 干等而只有一个在干活.
- Column Oriented 格式为 X = 200MB, 例如 Parquet.

**1. 新文件抵达 S3 Bucket, 将所有文件分割为较小的文件**

设原文件大小为 A.

1. 如果 A > 0.2X, 那么分割成 0.2X 大小的小文件.
2. 如果 A <= 0.2X, 那么不动.

设源文件大小为 B.


查询模式:

返回给定 dataset 以及 status 下的所有文件.

.. code-block:: python

    # pseudo code
    class File(Model):
        s3_uri = String(hash_key=True)
        dataset = String()
        status = Number()

    SELECT *
    FROM ind
    WHERE
        dataset = "events"
        AND status = 1
    LIMIT 100

    class Index(GSI):
        dataset







**Batch**






