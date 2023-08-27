variable "minio_region" {
  description = "Default MINIO region"
  default     = "us-west-1"
}

variable "minio_server" {
  description = "Default MINIO host and port"
  default     = "localhost:9000"
}

variable "minio_user" {
  description = "MINIO user"
  default     = "admin"
}

variable "minio_password" {
  description = "MINIO password"
  default     = "password"
}