variable "project_id" {
  description = "Project ID"
  type        = string
}

variable "region" {
  description = "Region resoruce"
  type        = string
  default     = "us-central1"
}

variable "dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
  default     = "instagram_data"
}