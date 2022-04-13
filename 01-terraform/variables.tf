locals {
  data_lake_bucket = "tda_data_lake"
}

variable "project" {
  description = "GCP Project ID"
  default="train-delays-analytics"
}

variable "region" {
  description = "europe-west6"
  default = "europe-west6"
  type = string
}

variable "bucket_name" {
    description = "tda_data_lake"
    default = "tda_data_lake"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "delays_data_all"
}