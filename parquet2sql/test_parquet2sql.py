import tempfile
import unittest

import pandas as pd
import parquet2sql


class TestParquet2SQL(unittest.TestCase):
    def test_parquet2sql(self):
        df = pd.DataFrame(columns=["one", "two", "three"],
                          data=[[1, "2", 3.0],
                                [4, "5", 6.0],
                                [7, "8", 9.0]])
        with tempfile.NamedTemporaryFile() as f:
            df.to_parquet(f.name)

            class args:
                parquet = f.name
                db = "postgresql://superset:superset@localhost:5432/superset"
                table = "parquet2db_test"

            parquet2sql.parquet2sql(args)

        df2 = pd.read_sql_table(args.table, args.db)
        del df2["index"]
        self.assertTrue(all(df2 == df))


if __name__ == '__main__':
    unittest.main()
