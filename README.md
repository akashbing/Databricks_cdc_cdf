# CDC Pipeline — MSK + Debezium + Glue (Terraform)

## Architecture

```
RDS / Aurora PostgreSQL  (logical replication / WAL)
    └── Debezium Connector  (MSK Connect — deployed by Terraform)
          └── Amazon MSK (Kafka)
                └── AWS Glue Streaming Job  (PySpark + Delta Lake)
                      ├── S3: raw CDC archive  (partitioned by date, append)
                      └── S3: current-state    (Delta MERGE, latest row per PK)
                            └── AWS Glue Data Catalog  (queryable via Athena)
```

## Project Structure

```
cdc-terraform/
├── main.tf                     # Provider, backend, random suffix
├── variables.tf                # All input variables
├── outputs.tf                  # Output values
├── terraform_resources.tf      # Root module — wires all child modules
├── terraform.tfvars.example    # Copy → terraform.tfvars and fill in
│
├── modules/
│   ├── networking/             # VPC, subnets, security groups
│   │   ├── main.tf
│   │   └── variables.tf
│   ├── s3/                     # All S3 buckets + script upload
│   │   ├── main.tf
│   │   └── variables.tf
│   ├── iam/                    # Glue role + MSK Connect role + policies
│   │   ├── main.tf
│   │   └── variables.tf
│   ├── msk/                    # MSK cluster + MSK Connect + Debezium
│   │   ├── main.tf
│   │   └── variables.tf
│   └── glue/                   # Glue job + connection + alerts + catalog
│       ├── main.tf
│       └── variables.tf
│
├── scripts/
│   └── cdc_glue_job.py         # Glue PySpark streaming job
│
└── plugins/                    # ← YOU must place Debezium ZIP here
    └── debezium-connector-postgres-2.4.0.zip
```

---

## Prerequisites

| Tool | Minimum version |
|---|---|
| Terraform | 1.5.0 |
| AWS CLI | 2.x |
| Python | 3.9+ |
| Java | 11+ (for local testing) |

---

## Step 1 — Prepare the Debezium plugin ZIP

Download the official Debezium PostgreSQL connector:

```bash
# Create the plugins directory
mkdir -p plugins

# Download Debezium connector
curl -L \
  https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.4.0.Final/debezium-connector-postgres-2.4.0.Final-plugin.tar.gz \
  | tar xz

# Create ZIP (MSK Connect requires ZIP format)
zip -r plugins/debezium-connector-postgres-2.4.0.zip debezium-connector-postgres/
```

---

## Step 2 — Prepare Extra JARs for Glue

Glue 4.0 needs these extra JARs for Delta Lake + MSK IAM auth.
Upload them to S3 after the scripts bucket is created:

```bash
# Download JARs
mkdir -p jars

# MSK IAM auth
curl -L -o jars/aws-msk-iam-auth-1.1.9-all.jar \
  https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar

# Delta Lake core
curl -L -o jars/delta-core_2.12-2.4.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar

# Delta storage
curl -L -o jars/delta-storage-2.4.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Upload to S3 (after terraform apply creates the bucket)
SCRIPTS_BUCKET=$(terraform output -raw scripts_s3_bucket)
aws s3 sync jars/ s3://${SCRIPTS_BUCKET}/jars/
```

---

## Step 3 — Configure RDS for logical replication

Run this **once** on your RDS PostgreSQL instance before deploying:

```sql
-- Requires rds.logical_replication=1 in your RDS parameter group

CREATE USER debezium_user REPLICATION LOGIN PASSWORD 'your_password';
GRANT CONNECT ON DATABASE myapp TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

CREATE PUBLICATION debezium_publication
  FOR TABLE public.orders, public.customers, public.products;
```

---

## Step 4 — Deploy with Terraform

```bash
# 1. Copy and edit the tfvars file
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your real values

# 2. Set the DB password via env var (do NOT put passwords in tfvars)
export TF_VAR_db_password="your_secure_password"

# 3. Initialise Terraform
terraform init

# 4. Plan — review what will be created
terraform plan -out=tfplan

# 5. Apply (creates all resources — takes ~15 minutes for MSK)
terraform apply tfplan
```

---

## Step 5 — Upload JARs and start the Glue job

```bash
# Upload JARs (if not done in Step 2)
SCRIPTS_BUCKET=$(terraform output -raw scripts_s3_bucket)
aws s3 sync jars/ s3://${SCRIPTS_BUCKET}/jars/

# Start the Glue streaming job
$(terraform output -raw run_glue_job_command)

# Verify it's running
aws glue get-job-runs \
  --job-name $(terraform output -raw glue_job_name) \
  --query 'JobRuns[0].{State:JobRunState,Started:StartedOn}' \
  --output table
```

---

## Monitoring

```bash
# View Glue job logs (real-time)
aws logs tail /aws/glue/jobs/cdc-pipeline-dev-cdc-streaming --follow

# View MSK Connect connector status
aws kafkaconnect describe-connector \
  --connector-arn $(terraform output -raw msk_connector_arn)

# Check Spark streaming query progress in Glue UI
# → AWS Console → Glue → Jobs → [job name] → Run → Spark UI

# Query output tables with Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM cdc_output.orders LIMIT 100" \
  --query-execution-context Database=cdc_output \
  --result-configuration OutputLocation=s3://$(terraform output -raw output_s3_bucket)/athena-results/
```

---

## Tear down

```bash
# Destroy all resources (irreversible!)
terraform destroy
```

---

## Environment promotion

```bash
# Deploy to staging
terraform workspace new staging
terraform apply -var="environment=staging" -var-file=envs/staging.tfvars

# Deploy to prod
terraform workspace new prod
terraform apply -var="environment=prod" -var-file=envs/prod.tfvars
```

---

## Adding a new table

1. Add the row schema and entry in `TABLE_CONFIG` in `scripts/cdc_glue_job.py`
2. Add the table to `db_tables` in `terraform.tfvars`
3. Run `terraform apply` to update the Debezium connector config
4. Re-upload the Glue script: `terraform apply` handles this automatically
5. Restart the Glue job
