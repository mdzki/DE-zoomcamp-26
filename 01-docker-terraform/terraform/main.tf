terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "terraform-demo-485018"
  region  = "europe-central2"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-demo-485018-terra-bucket"
  location      = "eu"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}