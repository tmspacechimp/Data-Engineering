def hdfs_cfg(spark):
    hdp = spark._jvm.org.apache.hadoop.fs
    fs = hdp.FileSystem.get(spark._jsc.hadoopConfiguration())

    return (fs, hdp.Path)


def list_files(path, spark):
    fs, path_type = hdfs_cfg(spark)
    path = path_type(path)
    files = fs.listFiles(path, False)
    result = []

    while files.hasNext():
        item = files.next()
        result.append(item.getPath().toString())

    return result


def find_filename(path, spark):
    for name in list_files(path, spark):
        if name.startswith(namenode + "/DataLake/fakestream_1/part-") \
                and name.endswith(".parquet"):
            return name
    return None


def delete_from_fs(filename, spark):
    fs, path_type = hdfs_cfg(spark)
    path = path_type(filename[len(namenode):])
    fs.delete(path, False)


namenode = "hdfs://namenode:8020"
