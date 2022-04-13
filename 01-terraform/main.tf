terraform {
  required_version = ">= 1.0"
  backend "gcs" {} 
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${var.bucket_name}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

#ToDo: Move table schemas to json files

# Fact table
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table
resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "train_delays_all"
  deletion_protection=false

  time_partitioning {
    field = "EVENT_DATETIME"
    type = "DAY"
  }

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "FINANCIAL_YEAR_AND_PERIOD",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Financial Year and Period - the “railway” period that the delay occurred in"
  },
  { 
    "name": "ORIGIN_DEPARTURE_DATE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "DD-MMM-YY Date – this is the date of the train within the system "
  },
  {
    "name": "TRUST_TRAIN_ID_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "TrainID – 8 digit trainid + the day of the month"
  },
  {
    "name": "PLANNED_ORIG_LOC_CODE_AFF",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Planned origin location code – stanox codes"
  },
  {
    "name": "PLANNED_ORIG_GBTT_DATETIME_AFF",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Planned departure datetime (Great Britain Timetable)"
  },
  {
    "name": "PLANNED_ORIG_WTT_DATETIME_AFF",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Planned departure datetime (Working Timetable)"
  },
  {
    "name": "PLANNED_DEST_LOC_CODE_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Planned destination location code – stanox codes"
  },
  {
    "name": "PLANNED_DEST_GBTT_DATETIME_AFF",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Planned departure datetime (Working Timetable)"
  },
  {
    "name": "PLANNED_DEST_WTT_DATETIME_AFF",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Planned departure datetime (Working Timetable)"
  },
  {
    "name": "TRAIN_SERVICE_CODE_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "TSC – this is the train service code at the point where the delay occurred"
  },
  {
    "name": "SERVICE_GROUP_CODE_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Service Group code – this is the service group within the Schedule 8 performance regime (and on Real Time PPM screens)"
  },
  {
    "name": "OPERATOR_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Operator – this is the operator code (ie TOC which ran the train)"
  },
  {
    "name": "ENGLISH_DAY_TYPE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "English Day Type – weekday, Saturday, Sunday, bank holiday, Christmas"
  },
  {
    "name": "APP_TIMETABLE_FLAG_AFF",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Applicable timetable flag – if N the train is not in official performance records as it is a short term replacement of a train plan – normally a reinstatement of part of a cancelled service"
  },
  {
    "name": "TRAIN_SCHEDULE_TYPE_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Train schedule type"
  },
  {
    "name": "TRACTION_TYPE_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Traction type"
  },
  {
    "name": "TRAILING_LOAD_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Trailing load"
  },
  {
    "name": "TIMING_LOAD_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "UNIT_CLASS_AFFECTED",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INCIDENT_NUMBER",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident number – the TRUST DA incident number (not unique without the create date)"
  },
  {
    "name": "INCIDENT_CREATE_DATE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident create date – the date the incident was entered into the system"
  },
  {
    "name": "INCIDENT_START_DATETIME",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident start date – the date the system has the incident live (this is not the length of the incident on the ground)"
  },
  {
    "name": "INCIDENT_END_DATETIME",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident end date – the date the system has the incident live (this is not the length of the incident on the ground)"
  },
  {
    "name": "SECTION_CODE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Section code – where the incident took place (combination of START_STANOX : END_STANOX)"
  },
  {
    "name": "NETWORK_RAIL_LOCATION_MANAGER",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Network Rail location Manager – the area of the country"
  },
  {
    "name": "RESPONSIBLE_MANAGER",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Responsible Manager – who within the industry is responsible for the delay – all delays have responsibility for performance improvement purposes"
  },
  {
    "name": "INCIDENT_REASON",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident Reason – the Delay Attribution Guide cause code for the incident"
  },
  {
    "name": "ATTRIBUTION_STATUS",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Status of an acceptance process"
  },
  {
    "name": "INCIDENT_EQUIPMENT",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident equipment – internal free form information"
  },
  {
    "name": "INCIDENT_DESCRIPTION",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident description – short description of the incident for internal use"
  },
  {
    "name": "REACTIONARY_REASON_CODE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Reactionary reason code. If no code the delay is primary (ie the delay is at the site of the incident) if reactionary the delay is a later consequence of that incident"
  },
  {
    "name": "INCIDENT_RESPONSIBLE_TRAIN",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Incident Responsible train – which train initially caused the incident (if any)"
  },
  {
    "name": "PERFORMANCE_EVENT_CODE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "A and M denote delays, C – full cancelletion, D diversion, F failure to stop, S scheduled cancellation, O/P part cancellations"
  },
  {
    "name": "START_STANOX",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Start stanox – the location of the delay (not the incident)"
  },
  {
    "name": "END_STANOX",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "End stanox – the location of the delay (not the incident)"
  },
  {
    "name": "EVENT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Event Datetime – the time the train encountered the delay"
  },
  {
    "name": "PFPI_MINUTES",
    "type": "FLOAT",
    "mode": "NULLABLE",
    "description": "The size of the delay. If the train is cancelleda deemed delay minute is generated for internal usage."
  },
  {
    "name": "TRUST_TRAIN_ID_RESP",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "TRUST_TRAIN_ID_REACT",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  }
]
EOF

}


# Dim tables

resource "google_bigquery_table" "dim_Stanox" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_Stanox"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "STANOX_NO",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "FULL_NAME",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "CRS_CODE",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Route_Description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}

resource "google_bigquery_table" "dim_Incident" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_Incident"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "Incident_Category",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Incident_Reason",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Incident_Reason_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Incident_Category_Description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Incident_Reason_Description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Incident_JPIP_Category",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Incident_Category_Super_Group_Code",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}
resource "google_bigquery_table" "dim_ResponsibleManager" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_ResponsibleManager"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "Responsible_Manager",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Responsible_Manager_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Responsible_Organisation",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Responsible_Organisation_Full_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Responsible_Organisation_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Responsible_Org_NR_TOC_FOC_Others",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}

resource "google_bigquery_table" "dim_ReactionaryReason" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_ReactionaryReason"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "Reactionary_Reason_Code",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Reactionary_Reason_Description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Reactionary_Reason_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}

resource "google_bigquery_table" "dim_PerformanceEvent" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_PerformanceEvent"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "Performance_Event_Types",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Performance_Event_Group",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Performance_Event_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}

resource "google_bigquery_table" "dim_ServiceGroup" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_ServiceGroup"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "Service_Group_Code",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Service_Group_Description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Train_Service_Code",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "Train_Service_Code_Description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}
resource "google_bigquery_table" "dim_Operator" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "dim_Operator"
  deletion_protection=false

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {  
    "name": "Operator_Code",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  { 
    "name": "Operator_Name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "INSERT_DATETIME",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "description": "Datetime when the dimension attributes were updated"
  }
]
  EOF
}