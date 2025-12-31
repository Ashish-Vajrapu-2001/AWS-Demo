output "control_db_endpoint" {
  value = aws_rds_cluster.control_db.endpoint
}

output "secret_arn" {
  value = aws_secretsmanager_secret.control_db_secret.arn
}

output "bucket_name" {
  value = aws_s3_bucket.bronze_bucket.bucket
}