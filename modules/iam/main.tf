# ─── modules/iam/main.tf ──────────────────────────────────────────────────────

# ── Glue Service Role ─────────────────────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name               = "${var.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  description        = "IAM role for CDC Glue streaming jobs"
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# Managed policies
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom inline policy
resource "aws_iam_role_policy" "glue_custom" {
  name   = "${var.name_prefix}-glue-custom-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_custom.json
}

data "aws_iam_policy_document" "glue_custom" {
  # ── S3 access ────────────────────────────────────────────────────────────
  statement {
    sid    = "S3BucketAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:ListBucket", "s3:GetBucketLocation",
    ]
    resources = concat(
      [for arn in var.s3_bucket_arns : arn],
      [for arn in var.s3_bucket_arns : "${arn}/*"],
    )
  }

  # ── MSK IAM auth ─────────────────────────────────────────────────────────
  statement {
    sid    = "MSKConnect"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:DescribeClusterDynamicConfiguration",
    ]
    resources = [
      var.msk_cluster_arn,
      "${replace(var.msk_cluster_arn, ":cluster/", ":topic/")}/*",
      "${replace(var.msk_cluster_arn, ":cluster/", ":group/")}/*",
    ]
  }

  statement {
    sid     = "MSKDescribe"
    effect  = "Allow"
    actions = ["kafka:DescribeCluster", "kafka:GetBootstrapBrokers"]
    resources = [var.msk_cluster_arn]
  }

  # ── Secrets Manager ───────────────────────────────────────────────────────
  statement {
    sid    = "SecretsManager"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = var.secret_arns
  }

  # ── CloudWatch Logs ───────────────────────────────────────────────────────
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/glue/*"]
  }

  # ── Glue Data Catalog ─────────────────────────────────────────────────────
  statement {
    sid    = "GlueCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions",
      "glue:CreatePartition", "glue:BatchCreatePartition",
      "glue:UpdatePartition", "glue:BatchDeletePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.aws_region}:${var.account_id}:table/${var.glue_database_name}/*",
    ]
  }

  # ── EC2 / VPC — needed for Glue connection ────────────────────────────────
  statement {
    sid    = "EC2VPC"
    effect = "Allow"
    actions = [
      "ec2:DescribeVpcs",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:AttachNetworkInterface",
    ]
    resources = ["*"]
  }

  # ── KMS (if using CMK) ────────────────────────────────────────────────────
  dynamic "statement" {
    for_each = var.kms_key_arn != "" ? [1] : []
    content {
      sid    = "KMSAccess"
      effect = "Allow"
      actions = [
        "kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt",
        "kms:DescribeKey", "kms:ReEncrypt*",
      ]
      resources = [var.kms_key_arn]
    }
  }
}

# ── MSK Connect Service Role ──────────────────────────────────────────────────
resource "aws_iam_role" "msk_connect" {
  name               = "${var.name_prefix}-msk-connect-role"
  assume_role_policy = data.aws_iam_policy_document.msk_connect_assume.json
  description        = "IAM role for MSK Connect (Debezium)"
}

data "aws_iam_policy_document" "msk_connect_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kafkaconnect.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "msk_connect_custom" {
  name   = "${var.name_prefix}-msk-connect-policy"
  role   = aws_iam_role.msk_connect.id
  policy = data.aws_iam_policy_document.msk_connect_custom.json
}

data "aws_iam_policy_document" "msk_connect_custom" {
  statement {
    sid    = "MSKClusterAccess"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:AlterCluster",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:CreateTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = [
      var.msk_cluster_arn,
      "${replace(var.msk_cluster_arn, ":cluster/", ":topic/")}/*",
      "${replace(var.msk_cluster_arn, ":cluster/", ":group/")}/*",
    ]
  }

  statement {
    sid     = "MSKDescribe"
    effect  = "Allow"
    actions = ["kafka:DescribeCluster", "kafka:GetBootstrapBrokers", "kafka:ListScramSecrets"]
    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "S3PluginAndLogs"
    effect = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket", "s3:PutObject"]
    resources = [
      var.scripts_bucket_arn,
      "${var.scripts_bucket_arn}/*",
      var.logs_bucket_arn,
      "${var.logs_bucket_arn}/*",
    ]
  }

  statement {
    sid     = "CloudWatchLogs"
    effect  = "Allow"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups"]
    resources = ["arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/msk-connect/*"]
  }

  statement {
    sid     = "SecretsManager"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = var.secret_arns
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "glue_role_arn"        { value = aws_iam_role.glue.arn }
output "glue_role_name"       { value = aws_iam_role.glue.name }
output "msk_connect_role_arn" { value = aws_iam_role.msk_connect.arn }
