provider "aws" {
  region = var.aws_region
}

# --- 1. Storage (S3) ---
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "bronze_ver" {
  bucket = aws_s3_bucket.bronze_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- 2. Control Database (RDS Aurora Serverless v2) ---
resource "aws_db_subnet_group" "control_db_subnet" {
  name       = "${var.project_name}-subnet-group"
  subnet_ids = var.subnet_ids
}

resource "aws_rds_cluster" "control_db" {
  cluster_identifier     = "${var.project_name}-control-db"
  engine                 = "aurora-postgresql"
  engine_mode            = "provisioned"
  engine_version         = "13.6"
  database_name          = "clv_control"
  master_username        = "dbadmin"
  master_password        = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.control_db_subnet.name
  skip_final_snapshot    = true
  
  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 1.0
  }
}

resource "aws_rds_cluster_instance" "control_db_instance" {
  cluster_identifier = aws_rds_cluster.control_db.id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.control_db.engine
  engine_version     = aws_rds_cluster.control_db.engine_version
}

# --- 3. Secrets Manager ---
resource "aws_secretsmanager_secret" "control_db_secret" {
  name = "clv/control/postgres"
}

resource "aws_secretsmanager_secret_version" "control_db_creds" {
  secret_id     = aws_secretsmanager_secret.control_db_secret.id
  secret_string = jsonencode({
    username = aws_rds_cluster.control_db.master_username
    password = aws_rds_cluster.control_db.master_password
    host     = aws_rds_cluster.control_db.endpoint
    dbname   = aws_rds_cluster.control_db.database_name
  })
}

# --- 4. IAM Roles ---
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_secrets" {
  name = "SecretsAccess"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["secretsmanager:GetSecretValue"]
      Resource = "*"
    }]
  })
}

# --- 5. Glue Jobs ---
# (Jobs are deployed via upload, this just creates the resource definition placeholder)
resource "aws_glue_job" "initial_load" {
  name     = "initial_load_dynamic"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  
  command {
    script_location = "s3://${aws_s3_bucket.bronze_bucket.bucket}/scripts/initial_load_dynamic.py"
  }
  
  default_arguments = {
    "--job-language" = "python"
    "--datalake-formats" = "delta"
    "--conf" = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }
}

resource "aws_glue_job" "incremental_cdc" {
  name     = "incremental_cdc_dynamic"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  
  command {
    script_location = "s3://${aws_s3_bucket.bronze_bucket.bucket}/scripts/incremental_cdc_dynamic.py"
  }

  default_arguments = {
    "--job-language" = "python"
    "--datalake-formats" = "delta"
    "--conf" = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }
}