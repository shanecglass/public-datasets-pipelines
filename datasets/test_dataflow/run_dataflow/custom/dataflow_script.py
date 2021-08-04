# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import calendar
import csv
import logging
import pathlib
import subprocess
from typing import Dict, List, Optional, Union

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# from apache_beam.typehints.typehints import Optional
from goes_reader import GOESMetadataReader


def generate_collection(
    target_bucket: str, target_prefix: str, satellite: str, product_type: str, year: int
) -> List[Dict[str, Union[pathlib.Path, int, str]]]:
    """Generates GCS path for the data source on a specified date,
    e.g. gs://gcp-public-data-goes-16/ABI-L1b-RadC/2020/005/01",
    """
    days_in_year = 366 if calendar.isleap(year) else 365

    collection = []
    for day in range(1, days_in_year + 1):
        for hour in range(24):
            collection.append(
                {
                    "source_bucket": f"gs://gcp-public-data-{satellite}",
                    "source_prefix": f"{product_type}/{year}/{day:03}/{hour:02}",
                    "target_bucket": target_bucket,
                    "target_prefix": target_prefix,
                    "local_dir": pathlib.Path(
                        f"/home/data/{satellite}/{product_type}/{year}/{day:03}/{hour:02}"
                    ),
                    "csv_path": pathlib.Path(
                        f"/home/data/{satellite}/{product_type}/{year}/{day:03}/{hour:02}__metadata.csv"
                    ),
                    "satellite": satellite,
                    "product_type": product_type,
                    "year": str(year),
                    "day": day,
                    "hour": hour,
                }
            )
    return collection


def download_gcs_prefix(
    col_item: Dict[str, Union[pathlib.Path, int, str]]
) -> Dict[str, str]:
    col_item["local_dir"].mkdir(parents=True, exist_ok=True)
    gcs_uri = f"gs://{col_item['source_bucket']}/{col_item['source_prefix']}"

    try:
        subprocess.check_call(
            ["gsutil", "-m", "cp", "-r", f"{gcs_uri}/*", col_item["local_dir"]]
        )
    except subprocess.CalledProcessError:
        raise FileNotFoundError
    return col_item


def read_metadata(col_item: Dict[str, str]) -> Dict[str, str]:
    product = col_item["product_type"]

    if "rad" in product.lower():
        file_type = "rad"
    elif "mcm" in product.lower():
        file_type = "mcm"
    elif "cmi" in product.lower():
        file_type = "cmi"
    elif "glm" in product.lower():
        file_type = "glm"
    else:
        raise ValueError("Unidentified file type for the product.")

    nc_files = [
        f
        for f in col_item["local_dir"].iterdir()
        if not f.is_dir() and f.suffix == ".nc"
    ]

    processor = GOESMetadataReader(file_type)
    with open(col_item["csv_path"], "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=",")
        headers = (
            processor.GetCsvHeader().replace("platform_ID", "platform_id").split(",")
        )
        writer.writerow(headers)

        for nc_file in nc_files:
            row = processor.GetMetadata(
                nc_file, col_item["source_prefix"], col_item["source_bucket"]
            )
            csv_row = processor.GetCsv(row)
            writer.writerow(csv_row.split(","))

    return col_item


def save_to_gcs(col_item: Dict[str, str]) -> None:
    gcs_uri = f"gs://{col_item['target_bucket']}/{col_item['target_prefix']}/{col_item['csv_path'].name}"
    subprocess.check_call(["gsutil", "cp", str(col_item["csv_path"]), gcs_uri])


def run(
    satellite: str,
    product_type: str,
    year: int,
    target_bucket: str,
    target_prefix: str,
    beam_pipeline_options: Optional[List[str]] = None,
) -> None:
    collection = generate_collection(
        target_bucket, target_prefix, satellite, product_type, year
    )

    options = PipelineOptions(beam_pipeline_options, save_main_session=True)
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Create GCS URIs" >> beam.Create(collection)
            | "Download .nc files"
            >> beam.Map(
                download_gcs_prefix,
            )
            | "Read metadata to CSV" >> beam.Map(read_metadata)
            | "Upload CSV files to GCS" >> beam.Map(save_to_gcs)
        )


if __name__ == "__main__":
    PRODUCTS = [
        "ABI-L1b-RadC",
        "ABI-L1b-RadF",
        "ABI-L1b-RadM",
        "ABI-L2-CMIPC",
        "ABI-L2-CMIPF",
        "ABI-L2-CMIPM",
        "ABI-L2-MCMIPC",
        "ABI-L2-MCMIPF",
        "ABI-L2-MCMIPM",
        "GLM-L2-LCFA",
    ]

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--satellite",
        required=True,
        type=str,
        choices=["goes-16", "goes-17"],
        dest="satellite",
        help="The GOES satellite name",
    )
    parser.add_argument(
        "--product_type",
        required=True,
        type=str,
        choices=PRODUCTS,
        dest="product_type",
        help="The product type",
    )
    parser.add_argument(
        "--year",
        required=True,
        type=int,
        dest="year",
        help="The year when the data was captured",
    )
    parser.add_argument(
        "--target_bucket",
        required=True,
        type=str,
        dest="target_bucket",
        help="The GCS target bucket to save the CSV files to",
    )
    parser.add_argument(
        "--target_prefix",
        required=True,
        type=str,
        dest="target_prefix",
        help="The GCS prefix in the target bucket to save the CSV files to",
    )
    custom_args, beam_pipeline_options = parser.parse_known_args()

    run(
        satellite=custom_args.satellite,
        product_type=custom_args.product_type,
        year=custom_args.year,
        target_bucket=custom_args.target_bucket,
        target_prefix=custom_args.target_prefix,
        beam_pipeline_options=beam_pipeline_options,
    )
