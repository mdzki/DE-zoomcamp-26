variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "project_name" {
  description = "Project name"
  default     = "terraform-demo-485018"
}

variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "region" {
  description = "Provider's region"
  default     = "europe-central2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Class"
  default     = "terraform-demo-485018-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

