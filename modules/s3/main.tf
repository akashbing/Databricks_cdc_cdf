# ─── modules/s3/main.tf ───────────────────────────────────────────────────────

locals {
  buckets = {
    scripts     = "${var.name_prefix}-scripts-${var.suffix}"
    output      = "${var.name_prefix}-output-${var.suffix}"
    checkpoints = "${var.name_prefix}-checkpoints-${var.suffix}"
    logs        = "${var.name_prefix}-logs-${var.suffix}"
    dlq         = "${var.name_prefix}-dlq-${var.suffix}"
    spark_ui    = "${var.name_prefix}-spark-ui-${var.suffix}"
  }
}

# ── Create all buckets ────────────────────────────────────────────────────────
resource "aws_s3_bucket" "buckets" {
  for_each = local.buckets
  bucket   = each.value
  tags     = { Purpose = each.key }
}

# ── Block all public access ───────────────────────────────────────────────────
resource "aws_s3_bucket_public_access_block" "buckets" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.buckets[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Enable versioning on scripts and output ───────────────────────────────────
resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.buckets["scripts"].id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "output" {
  bucket = aws_s3_bucket.buckets["output"].id
  versioning_configuration { status = "Enabled" }
}

# ── Server-side encryption ────────────────────────────────────────────────────
resource "aws_s3_bucket_server_side_encryption_configuration" "buckets" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
    }
    bucket_key_enabled = var.kms_key_arn != "" ? true : false
  }
}

# ── Lifecycle rules — expire checkpoints and logs ─────────────────────────────
resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.buckets["checkpoints"].id

  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 30 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.buckets["logs"].id

  rule {
    id     = "expire-logs"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 90 }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "dlq" {
  bucket = aws_s3_bucket.buckets["dlq"].id

  rule {
    id     = "expire-dlq"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 60 }
  }
}

# ── Upload Glue PySpark script ────────────────────────────────────────────────
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.buckets["scripts"].id
  key    = "glue-scripts/cdc_glue_job.py"
  source = "${path.root}/scripts/cdc_glue_job.py"
  etag   = filemd5("${path.root}/scripts/cdc_glue_job.py")
}

# ── Upload extra Python libs if needed ───────────────────────────────────────
resource "aws_s3_object" "glue_extra_libs" {
  for_each = toset(var.glue_extra_py_files)
  bucket   = aws_s3_bucket.buckets["scripts"].id
  key      = "glue-scripts/libs/${basename(each.value)}"
  source   = each.value
  etag     = filemd5(each.value)
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "scripts_bucket_id"      { value = aws_s3_bucket.buckets["scripts"].id }
output "scripts_bucket_arn"     { value = aws_s3_bucket.buckets["scripts"].arn }
output "output_bucket_id"       { value = aws_s3_bucket.buckets["output"].id }
output "output_bucket_arn"      { value = aws_s3_bucket.buckets["output"].arn }
output "checkpoints_bucket_id"  { value = aws_s3_bucket.buckets["checkpoints"].id }
output "checkpoints_bucket_arn" { value = aws_s3_bucket.buckets["checkpoints"].arn }
output "logs_bucket_id"         { value = aws_s3_bucket.buckets["logs"].id }
output "logs_bucket_arn"        { value = aws_s3_bucket.buckets["logs"].arn }
output "dlq_bucket_id"          { value = aws_s3_bucket.buckets["dlq"].id }
output "spark_ui_bucket_id"     { value = aws_s3_bucket.buckets["spark_ui"].id }
output "glue_script_s3_path"    { value = "s3://${aws_s3_bucket.buckets["scripts"].id}/${aws_s3_object.glue_script.key}" }
