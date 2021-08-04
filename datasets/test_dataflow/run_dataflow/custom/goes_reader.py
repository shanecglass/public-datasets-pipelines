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


import csv
import io
import os
import pathlib

import h5py

ATTRS_KEYS = {
    "rad": [
        "dataset_name",
        "platform_ID",
        "orbital_slot",
        "timeline_id",
        "scene_id",
        "time_coverage_start",
        "time_coverage_end",
        "date_created",
    ],
    "cmi": [
        "dataset_name",
        "platform_ID",
        "orbital_slot",
        "timeline_id",
        "scene_id",
        "time_coverage_start",
        "time_coverage_end",
        "date_created",
    ],
    "mcm": [
        "dataset_name",
        "platform_ID",
        "orbital_slot",
        "timeline_id",
        "scene_id",
        "time_coverage_start",
        "time_coverage_end",
        "date_created",
    ],
    "glm": [
        "dataset_name",
        "platform_ID",
        "orbital_slot",
        "time_coverage_start",
        "time_coverage_end",
        "date_created",
    ],
}

GEO_LAT_LON_KEYS = {
    "rad": [
        "geospatial_westbound_longitude",
        "geospatial_northbound_latitude",
        "geospatial_eastbound_longitude",
        "geospatial_southbound_latitude",
    ],
    "cmi": [
        "geospatial_westbound_longitude",
        "geospatial_northbound_latitude",
        "geospatial_eastbound_longitude",
        "geospatial_southbound_latitude",
    ],
    "mcm": [
        "geospatial_westbound_longitude",
        "geospatial_northbound_latitude",
        "geospatial_eastbound_longitude",
        "geospatial_southbound_latitude",
    ],
    "glm": [],
}

VALUE_KEYS = {
    "rad": [
        "band_id",
        "nominal_satellite_subpoint_lon",
        "valid_pixel_count",
        "missing_pixel_count",
        "saturated_pixel_count",
        "undersaturated_pixel_count",
        "min_radiance_value_of_valid_pixels",
        "max_radiance_value_of_valid_pixels",
        "mean_radiance_value_of_valid_pixels",
        "std_dev_radiance_value_of_valid_pixels",
        "percent_uncorrectable_L0_errors",
    ],
    "cmi": [
        "band_id",
        "nominal_satellite_subpoint_lon",
        "total_number_of_points",
        "valid_pixel_count",
        "outlier_pixel_count",
        "min_reflectance_factor",
        "max_reflectance_factor",
        "mean_reflectance_factor",
        "std_dev_reflectance_factor",
        "min_brightness_temperature",  # Missing attribute
        "max_brightness_temperature",  # Missing attribute
        "mean_brightness_temperature",  # Missing attribute
        "std_dev_brightness_temperature",  # Missing attribute
        "percent_uncorrectable_GRB_errors",
        "percent_uncorrectable_L0_errors",
    ],
    "mcm": [
        "nominal_satellite_subpoint_lon",
        "outlier_pixel_count_C01",
        "outlier_pixel_count_C02",
        "outlier_pixel_count_C03",
        "outlier_pixel_count_C04",
        "outlier_pixel_count_C05",
        "outlier_pixel_count_C06",
        "outlier_pixel_count_C07",
        "outlier_pixel_count_C08",
        "outlier_pixel_count_C09",
        "outlier_pixel_count_C10",
        "outlier_pixel_count_C11",
        "outlier_pixel_count_C12",
        "outlier_pixel_count_C13",
        "outlier_pixel_count_C14",
        "outlier_pixel_count_C15",
        "outlier_pixel_count_C16",
        "min_reflectance_factor_C01",
        "max_reflectance_factor_C01",
        "mean_reflectance_factor_C01",
        "std_dev_reflectance_factor_C01",
        "min_reflectance_factor_C02",
        "max_reflectance_factor_C02",
        "mean_reflectance_factor_C02",
        "std_dev_reflectance_factor_C02",
        "min_reflectance_factor_C03",
        "max_reflectance_factor_C03",
        "mean_reflectance_factor_C03",
        "std_dev_reflectance_factor_C03",
        "min_reflectance_factor_C04",
        "max_reflectance_factor_C04",
        "mean_reflectance_factor_C04",
        "std_dev_reflectance_factor_C04",
        "min_reflectance_factor_C05",
        "max_reflectance_factor_C05",
        "mean_reflectance_factor_C05",
        "std_dev_reflectance_factor_C05",
        "min_reflectance_factor_C06",
        "max_reflectance_factor_C06",
        "mean_reflectance_factor_C06",
        "std_dev_reflectance_factor_C06",
        "min_brightness_temperature_C06",
        "max_brightness_temperature_C06",
        "mean_brightness_temperature_C06",
        "std_dev_brightness_temperature_C06",
        "min_brightness_temperature_C07",
        "max_brightness_temperature_C07",
        "mean_brightness_temperature_C07",
        "std_dev_brightness_temperature_C07",
        "min_brightness_temperature_C08",
        "max_brightness_temperature_C08",
        "mean_brightness_temperature_C08",
        "std_dev_brightness_temperature_C08",
        "min_brightness_temperature_C09",
        "max_brightness_temperature_C09",
        "mean_brightness_temperature_C09",
        "std_dev_brightness_temperature_C09",
        "min_brightness_temperature_C10",
        "max_brightness_temperature_C10",
        "mean_brightness_temperature_C10",
        "std_dev_brightness_temperature_C10",
        "min_brightness_temperature_C11",
        "max_brightness_temperature_C11",
        "mean_brightness_temperature_C11",
        "std_dev_brightness_temperature_C11",
        "min_brightness_temperature_C12",
        "max_brightness_temperature_C12",
        "mean_brightness_temperature_C12",
        "std_dev_brightness_temperature_C12",
        "min_brightness_temperature_C13",
        "max_brightness_temperature_C13",
        "mean_brightness_temperature_C13",
        "std_dev_brightness_temperature_C13",
        "min_brightness_temperature_C14",
        "max_brightness_temperature_C14",
        "mean_brightness_temperature_C14",
        "std_dev_brightness_temperature_C14",
        "min_brightness_temperature_C15",
        "max_brightness_temperature_C15",
        "mean_brightness_temperature_C15",
        "std_dev_brightness_temperature_C15",
        "min_brightness_temperature_C16",
        "max_brightness_temperature_C16",
        "mean_brightness_temperature_C16",
        "std_dev_brightness_temperature_C16",
        "percent_uncorrectable_GRB_errors",
        "percent_uncorrectable_L0_errors",
    ],
    "glm": [
        "group_time_threshold",
        "flash_time_threshold",
        "event_count",
        "group_count",
        "flash_count",
        "percent_navigated_L1b_events",
        "percent_uncorrectable_L0_errors",
    ],
}

FILE_TYPE_TO_TABLE = {
    "rad": "abi_l1b_radiance",
    "cmi": "abi_l2_cmip",
    "mcm": "abi_l2_mcmip",
    "glm": "glm_l2_lcfa",
}


def decode_bytes(obj):
    if isinstance(obj, bytes):
        return obj.decode()
    else:
        return obj


class GOESMetadataReader(object):
    """A class for reading GOES metadata and writing them to csv file."""

    def __init__(self, file_type):
        # Configure the columns to be extracted
        self._file_type = file_type
        self._attr_keys = ATTRS_KEYS[self._file_type]
        self._geo_lat_lon_keys = GEO_LAT_LON_KEYS[self._file_type]
        self._value_keys = VALUE_KEYS[self._file_type]

        self._fieldnames = []
        self._fieldnames.extend(self._attr_keys)
        if self._file_type != "glm":
            self._fieldnames.extend(self._geo_lat_lon_keys)
        self._fieldnames.extend(self._value_keys)
        self._fieldnames.extend(["total_size", "base_url"])

    def GetMetadata(self, file_path: pathlib.Path, gcs_prefix: str, gcs_bucket: str):
        """Reads input file for metadata."""
        self._md_dict = {}
        with h5py.File(file_path, "r") as f:
            # Top level properties
            self._md_dict.update(
                {
                    k: decode_bytes(v.item())
                    for k, v in f.attrs.items()
                    if k in self._attr_keys
                }
            )
            # Geospatial Lat Lon related properties
            if self._file_type != "glm":
                self._md_dict.update(
                    {
                        k: decode_bytes(v.item())
                        for k, v in f["geospatial_lat_lon_extent"].attrs.items()
                        if k in self._geo_lat_lon_keys
                    }
                )
            # Value properties
            self._md_dict.update(
                {k: v[()] for k, v in f.items() if k in self._value_keys}
            )
            total_size = os.path.getsize(file_path)
            base_url = f"gs://{gcs_bucket}/{gcs_prefix}/{file_path.name}"
            self._md_dict.update({"total_size": total_size, "base_url": base_url})

        # os.remove(temp_file)
        return self._md_dict

    def GetCsv(self, row):
        returnval = ""
        output = io.StringIO()
        tmpwriter = csv.DictWriter(
            output, fieldnames=self._fieldnames, lineterminator=""
        )
        tmpwriter.writerow(row)
        returnval = output.getvalue()
        output.close()
        return returnval

    def GetCsvHeader(self):
        returnval = ""
        output = io.StringIO()
        tmpwriter = csv.DictWriter(
            output, fieldnames=self._fieldnames, lineterminator=""
        )
        tmpwriter.writeheader()
        returnval = output.getvalue()
        output.close()
        return returnval
