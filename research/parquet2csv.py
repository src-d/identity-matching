import argparse
from pathlib import Path

from pyspark.sql import SparkSession, SQLContext


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Utility to convert parquet file to csv.")
    parser.add_argument("input", help="input parquet file")
    parser.add_argument("--output", default=None, required=False,
                        help="output directory with a csv file inside."
                             "Use the same name with .csv suffix by default.")
    parser.add_argument("-f", "--force", default=False, action="store_true",
                        help="Override output file if exists.")
    parser.add_argument("-m", "--memory", default="4gb", required=False,
                        help="spark.executor.memory spark configuration value.")
    parser.add_argument("-c", "--cores", default="4", required=False,
                        help="spark.cores.max spark configuration value.")

    args = parser.parse_args()

    input_path = Path(args.input)
    output = Path(args.output) if args.output else input_path.with_suffix(".csv")
    if not args.force and output.exists():
        print("File %s exists. Set -f flag to overwrite." % str(output))
        exit(1)

    spark = SparkSession.builder \
        .master("local") \
        .appName("parquet2csv") \
        .config("spark.executor.memory", args.memory) \
        .config("spark.cores.max", args.cores) \
        .getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)
    df = sqlContext.read.parquet(args.input)
    df.write.csv(str(output), mode="overwrite", header=True)
    print("CSV file saved to %s" % str(output))
