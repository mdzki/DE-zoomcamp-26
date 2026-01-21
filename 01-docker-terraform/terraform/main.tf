terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
    project     = "terraform-demo-485018"
    region      = "europe-central2"
}

