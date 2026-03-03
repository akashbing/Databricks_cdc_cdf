# ─── networking/main.tf ───────────────────────────────────────────────────────
# Creates or references VPC, subnets, and security groups
# used by MSK, MSK Connect, and Glue.

locals {
  create_vpc     = var.vpc_id == ""
  create_subnets = length(var.private_subnet_ids) == 0
  vpc_id         = local.create_vpc ? aws_vpc.this[0].id : var.vpc_id
  subnet_ids     = local.create_subnets ? aws_subnet.private[*].id : var.private_subnet_ids
}

# ── VPC (only when not supplied) ──────────────────────────────────────────────
resource "aws_vpc" "this" {
  count                = local.create_vpc ? 1 : 0
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = { Name = "${var.name_prefix}-vpc" }
}

resource "aws_internet_gateway" "this" {
  count  = local.create_vpc ? 1 : 0
  vpc_id = aws_vpc.this[0].id
  tags   = { Name = "${var.name_prefix}-igw" }
}

# ── Private subnets ───────────────────────────────────────────────────────────
resource "aws_subnet" "private" {
  count             = local.create_subnets ? length(var.availability_zones) : 0
  vpc_id            = local.vpc_id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone = var.availability_zones[count.index]
  tags = { Name = "${var.name_prefix}-private-${var.availability_zones[count.index]}" }
}

# ── NAT Gateway (for Glue / MSK Connect to reach internet) ───────────────────
resource "aws_eip" "nat" {
  count  = local.create_vpc ? 1 : 0
  domain = "vpc"
  tags   = { Name = "${var.name_prefix}-nat-eip" }
}

resource "aws_subnet" "public" {
  count                   = local.create_vpc ? 1 : 0
  vpc_id                  = local.vpc_id
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, 10)
  availability_zone       = var.availability_zones[0]
  map_public_ip_on_launch = true
  tags = { Name = "${var.name_prefix}-public" }
}

resource "aws_nat_gateway" "this" {
  count         = local.create_vpc ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id
  depends_on    = [aws_internet_gateway.this]
  tags          = { Name = "${var.name_prefix}-nat" }
}

resource "aws_route_table" "private" {
  count  = local.create_vpc ? 1 : 0
  vpc_id = local.vpc_id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[0].id
  }
  tags = { Name = "${var.name_prefix}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = local.create_subnets ? length(var.availability_zones) : 0
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

# ── Security Groups ───────────────────────────────────────────────────────────

# MSK Brokers
resource "aws_security_group" "msk" {
  name        = "${var.name_prefix}-msk-sg"
  description = "Allow Kafka traffic to MSK brokers"
  vpc_id      = local.vpc_id

  ingress {
    description     = "Kafka plaintext from Glue and MSK Connect"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [aws_security_group.glue.id, aws_security_group.msk_connect.id]
  }

  ingress {
    description     = "Kafka TLS"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.glue.id, aws_security_group.msk_connect.id]
  }

  ingress {
    description     = "Kafka IAM / SASL_SSL"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.glue.id, aws_security_group.msk_connect.id]
  }

  ingress {
    description = "Broker-to-broker"
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-msk-sg" }
}

# Glue Jobs
resource "aws_security_group" "glue" {
  name        = "${var.name_prefix}-glue-sg"
  description = "Security group for AWS Glue streaming jobs"
  vpc_id      = local.vpc_id

  # Glue requires self-referencing rule for internal communication
  ingress {
    description = "Glue self-referencing"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-glue-sg" }
}

# MSK Connect Workers
resource "aws_security_group" "msk_connect" {
  name        = "${var.name_prefix}-msk-connect-sg"
  description = "Security group for MSK Connect workers"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-msk-connect-sg" }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "vpc_id"              { value = local.vpc_id }
output "private_subnet_ids"  { value = local.subnet_ids }
output "msk_sg_id"           { value = aws_security_group.msk.id }
output "glue_sg_id"          { value = aws_security_group.glue.id }
output "msk_connect_sg_id"   { value = aws_security_group.msk_connect.id }
