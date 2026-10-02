from pyspark.sql import functions as F


def clean_data(df):
    cleaned_df = df.filter(
        (F.col("amount") > 0) &
        (F.col("name").isNotNull())
    )

    cleaned_df = cleaned_df.withColumn(
        "amount_with_tax",
        F.col("amount") * 1.20
    )

    return cleaned_df