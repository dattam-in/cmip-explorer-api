import argparse
import math
import os
import psutil
import pyspark
from chardet.universaldetector import UniversalDetector
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def get_spark_client(name, tmp_dir, cpu_factor=0.9, memory_factor=0.8):
    cpu_cores = psutil.cpu_count()
    ram = math.floor(psutil.virtual_memory().total / (1024**3))
    spark_ram = math.floor(ram * memory_factor)
    spark_cores = math.floor(cpu_cores * cpu_factor)
    assert spark_ram >= 1, "Spark RAM should be at least 1GB"
    assert spark_cores >= 1, "Spark cores should be at least 1"
    spark = (
        SparkSession.builder.config("spark.worker.cleanup.enabled", "true")
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config("spark.local.dir", tmp_dir)
        .config("spark.driver.memory", f"{spark_ram}G")
        .config("spark.executor.cores", spark_cores)
        .master(f"local[{spark_cores}]")
        .appName(name)
        .getOrCreate()
    )
    print(
        f"Spark configured with {spark_cores} cores and {spark_ram}GB RAM, with temp dir {tmp_dir}"
    )
    return spark


def convert_empty_arrays_to_null(df):
    # handle top level arrays and dicts. This doesnt handle arrays of objects.
    schema = df.schema
    for field in schema.fields:
        if isinstance(field.dataType, pyspark.sql.types.ArrayType):
            df = df.withColumn(
                field.name,
                F.when(F.size(F.col(field.name)) > 0, F.col(field.name)).otherwise(
                    None
                ),
            )
        elif isinstance(field.dataType, pyspark.sql.types.StructType):
            for subfield in field.dataType.fields:
                if isinstance(subfield.dataType, pyspark.sql.types.ArrayType):
                    df = df.withColumn(
                        field.name,
                        F.col(field.name).withField(
                            subfield.name,
                            F.when(
                                F.size(F.col(field.name)[subfield.name]) > 0,
                                F.col(field.name)[subfield.name],
                            ).otherwise(None),
                        ),
                    )
    return df


def str2bool(v):
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def detect_file_encoding(src):
    detector = UniversalDetector()
    with open(src, "rb") as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        enc = detector.result["encoding"]
        return enc
