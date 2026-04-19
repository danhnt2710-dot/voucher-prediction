from pyspark.sql.functions import col, avg, count, round, when, broadcast, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# ==================== SCHEMA ====================
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", IntegerType(), True),
    StructField("order_hour_of_day", IntegerType(), True),
    StructField("days_since_prior_order", FloatType(), True)
])

# ==================== LỚP 1: NGUỒN DỮ LIỆU ====================
def read_static_data(spark, base_path):
    """Đọc toàn bộ dữ liệu tĩnh từ UC Volume"""
    orders_df = spark.read.format("csv").option("header", True) \
        .load(f"{base_path}/orders.csv") \
        .withColumn("user_id", expr("try_cast(user_id as int)")) \
        .withColumn("order_id", expr("try_cast(order_id as int)")) \
        .withColumn("order_number", expr("try_cast(order_number as int)")) \
        .withColumn("order_dow", expr("try_cast(order_dow as int)")) \
        .withColumn("order_hour_of_day", expr("try_cast(order_hour_of_day as int)")) \
        .withColumn("days_since_prior_order", expr("try_cast(days_since_prior_order as float)"))

    order_products_df = spark.read.format("csv").option("header", True) \
        .load(f"{base_path}/order_products__prior.csv") \
        .withColumn("order_id", expr("try_cast(order_id as int)")) \
        .withColumn("product_id", expr("try_cast(product_id as int)"))

    products_df = spark.read.format("csv").option("header", True) \
        .load(f"{base_path}/products.csv") \
        .withColumn("product_id", expr("try_cast(product_id as int)")) \
        .withColumn("department_id", expr("try_cast(department_id as int)"))

    departments_df = spark.read.format("csv").option("header", True) \
        .load(f"{base_path}/departments.csv") \
        .withColumn("department_id", expr("try_cast(department_id as int)"))

    return orders_df, order_products_df, products_df, departments_df

# ==================== LỚP 2: TIẾP NHẬN DỮ LIỆU ====================
def create_stream(spark, streaming_path):
    """Tạo streaming DataFrame từ UC Volume"""
    return spark.readStream \
        .format("csv") \
        .option("header", True) \
        .option("maxFilesPerTrigger", 2) \
        .schema(orders_schema) \
        .load(f"{streaming_path}/input/orders")

# ==================== LỚP 3: XỬ LÝ DỮ LIỆU ====================
def build_user_profile(orders_df, order_products_df, products_df, departments_df):
    """Tạo bảng profile user từ dữ liệu tĩnh"""
    user_time_behavior = orders_df \
        .filter(col("days_since_prior_order").isNotNull()) \
        .filter(col("days_since_prior_order") > 0) \
        .groupBy("user_id") \
        .agg(
            avg("days_since_prior_order").alias("avg_cycle_days"),
            avg("order_hour_of_day").alias("avg_hour"),
            avg("order_dow").alias("avg_dow"),
            count("order_id").alias("total_orders")
        ) \
        .filter(col("avg_cycle_days") > 0)

    user_department = orders_df \
        .join(order_products_df, "order_id") \
        .join(products_df, "product_id") \
        .join(departments_df, "department_id") \
        .groupBy("user_id", "department") \
        .agg(count("product_id").alias("buy_count")) \
        .withColumn(
            "rank",
            row_number().over(
                Window.partitionBy("user_id").orderBy(col("buy_count").desc())
            )
        ) \
        .filter(col("rank") == 1) \
        .select("user_id", col("department").alias("fav_department"))

    return user_time_behavior.join(user_department, "user_id")

def compute_cycle_progress(df):
    """Tính tiến độ chu kỳ mua hàng"""
    return df.withColumn(
        "cycle_progress",
        round(expr("try_divide(days_since_prior_order, avg_cycle_days)"), 2)
    )

def filter_valid_orders(df):
    """Lọc bỏ các đơn hàng không hợp lệ"""
    return df \
        .filter(col("days_since_prior_order") > 0) \
        .filter(col("avg_cycle_days") >= 3) \
        .filter(col("cycle_progress").isNotNull())

# ==================== LỚP 4: TỐI ƯU ====================
def classify_golden_moment(df):
    """Phân loại thời điểm vàng gửi voucher"""
    return df.withColumn(
        "golden_moment",
        when(col("cycle_progress") >= 1.67, "🔴 Gửi ngay - Quá hạn mua!")
        .when(col("cycle_progress") >= 1.31, "🟡 Gửi sớm - Sắp đến chu kỳ")
        .otherwise("🟢 Chưa cần gửi")
    )

def get_best_hour(df):
    """Tính giờ vàng gửi voucher"""
    return df.withColumn(
        "best_hour_to_send",
        round(col("avg_hour"), 0).cast("int")
    )

def get_best_day(df):
    """Tính ngày vàng gửi voucher"""
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

def build_golden_stream(orders_stream, user_profile_static):
    """Xây dựng streaming pipeline hoàn chỉnh"""
    return orders_stream \
        .join(broadcast(user_profile_static), "user_id") \
        .transform(compute_cycle_progress) \
        .transform(filter_valid_orders) \
        .transform(classify_golden_moment) \
        .transform(get_best_hour) \
        .transform(get_best_day) \
        .select(
            "user_id", "days_since_prior_order", "avg_cycle_days",
            "cycle_progress", "golden_moment", "best_hour_to_send",
            "best_day_to_send", "fav_department"
        )

# ==================== LỚP 5: LƯU TRỮ ====================
def write_stream(golden_moment_stream, streaming_path, query_name="golden_voucher"):
    """Ghi stream ra memory"""
    return golden_moment_stream.writeStream \
        .format("memory") \
        .queryName(query_name) \
        .outputMode("append") \
        .trigger(availableNow=True) \
        .option("checkpointLocation", f"{streaming_path}/checkpoint/golden_moment") \
        .start()

# ==================== LỚP 6: GIÁM SÁT ====================
def monitor_stream(query, spark, query_name="golden_voucher"):
    """Giám sát stream và in metrics"""
    actual_count = spark.sql(f"SELECT COUNT(*) FROM {query_name}").collect()[0][0]

    print("\n📊 STATUS:")
    print(query.status)

    progress = query.lastProgress
    if progress is not None:
        batch_id = progress.get("batchId", 0) or 0
        duration = progress.get("durationMs", {}) or {}
        print(f"\n📈 CHỈ SỐ:")
        print(f"  Batch ID:          {batch_id}")
        print(f"  Số dòng thực tế:   {actual_count:,}")
        print(f"  Thời gian xử lý:   {duration}")

    print(f"\n⚠️ ALERT CHECK:")
    if actual_count == 0:
        print("  ⚠️ CẢNH BÁO: Không có dữ liệu mới!")
    else:
        print(f"  ✅ Đã xử lý {actual_count:,} rows")

    print("\n✅ Stream hoàn tất")
    return actual_count
