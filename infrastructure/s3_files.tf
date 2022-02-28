resource "aws_s3_bucket_object" "delta_insert" {
  bucket = aws_s3_bucket.datalakehouse.id
  key    = "emr-code/pyspark/delta_spark_insert_enem2019.py"
  acl    = "private"
  source = "../etl/delta_spark_insert_enem2019.py"
  etag   = filemd5("../etl/delta_spark_insert_enem2019.py")
}

resource "aws_s3_bucket_object" "delta_upsert" {
  bucket = aws_s3_bucket.datalakehouse.id
  key    = "emr-code/pyspark/delta_spark_upsert_enem2019.py"
  acl    = "private"
  source = "../etl/delta_spark_upsert_enem2019.py"
  etag   = filemd5("../etl/delta_spark_upsert_enem2019.py")
}