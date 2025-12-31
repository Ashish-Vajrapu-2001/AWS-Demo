variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "clv-bronze"
}

variable "bucket_name" {
  description = "Name of the Bronze S3 Bucket"
  default     = "myntra-clv-bronze"
}

variable "vpc_id" {
  description = "VPC ID where Glue and RDS reside"
  type        = string
}

variable "subnet_ids" {
  description = "List of Subnet IDs for Glue"
  type        = list(string)
}

variable "db_password" {
  description = "Master password for Control DB"
  type        = string
  sensitive   = true
}