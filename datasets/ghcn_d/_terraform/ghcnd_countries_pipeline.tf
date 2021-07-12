/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


resource "google_bigquery_table" "ghcnd_countries" {
  project    = var.project_id
  dataset_id = "ghcn_d"
  table_id   = "ghcnd_countries"

  description = "ghcn_dspc"

  depends_on = [
    google_bigquery_dataset.ghcn_d
  ]
}

output "bigquery_table-ghcnd_countries-table_id" {
  value = google_bigquery_table.ghcnd_countries.table_id
}

output "bigquery_table-ghcnd_countries-id" {
  value = google_bigquery_table.ghcnd_countries.id
}
