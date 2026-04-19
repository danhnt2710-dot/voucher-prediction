from pyspark.sql.functions import col, round, when, expr

def compute_cycle_progress(df):
    return df.withColumn(
        "cycle_progress",
        round(expr("try_divide(days_since_prior_order, avg_cycle_days)"), 2)
    )

def classify_golden_moment(df):
    return df.withColumn(
        "golden_moment",
        when(col("cycle_progress") >= 1.67, "🔴 Gửi ngay - Quá hạn mua!")
        .when(col("cycle_progress") >= 1.31, "🟡 Gửi sớm - Sắp đến chu kỳ")
        .otherwise("🟢 Chưa cần gửi")
    )

def get_best_day(df):
    return df.withColumn(
        "best_day_to_send",
        when(col("avg_dow") < 0.5, "Chủ nhật")
        .when(col("avg_dow") < 1.5, "Thứ 2")
        .when(col("avg_dow") < 2.5, "Thứ 3")
        .when(col("avg_dow") < 3.5, "Thứ 4")
        .when(col("avg_dow") < 4.5, "Thứ 5")
        .when(col("avg_dow") < 5.5, "Thứ 6")
        .otherwise("Thứ 7")
    )

def filter_valid_orders(df):
    return df \
        .filter(col("days_since_prior_order") > 0) \
        .filter(col("avg_cycle_days") >= 3) \
        .filter(col("cycle_progress").isNotNull())
