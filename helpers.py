"""Shared utilities for all boto3/moto tutorial notebooks."""

import io
import json
import os

import boto3
import pandas as pd
import requests

DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/dummy-role"

# Set dummy credentials so boto3 never raises NoCredentialsError inside moto mocks.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


def make_boto3_client(service: str, region: str = "us-east-1"):
    """Return a boto3 client with dummy credentials pre-configured."""
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def upload_df_as_csv(s3_client, df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload a pandas DataFrame to S3 as a UTF-8 CSV."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))


def upload_df_as_parquet(s3_client, df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload a pandas DataFrame to S3 as Parquet (requires pyarrow)."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def download_df_from_csv(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Download an S3 object and parse it as a CSV DataFrame."""
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(resp["Body"].read()))


def download_df_from_parquet(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Download an S3 object and parse it as a Parquet DataFrame."""
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))


def inject_athena_results(
    rows: list[list],
    column_info: list[dict],
    region: str = "us-east-1",
) -> None:
    """
    Pre-load query results into the moto Athena mock via its internal REST endpoint.

    rows        -- list of row value lists, e.g. [["Alice", "30"], ["Bob", "25"]]
    column_info -- list of {"Name": ..., "Type": ...} dicts matching the column order
    """
    payload = {
        "results": [
            {
                "rows": [{"Data": [{"VarCharValue": str(v)} for v in row]} for row in rows],
                "column_info": column_info,
            }
        ]
    }
    resp = requests.post(
        f"http://motoapi.amazonaws.com/moto-api/static/athena/query-results",
        json=payload,
    )
    resp.raise_for_status()


def parse_athena_results(query_results: dict) -> pd.DataFrame:
    """
    Convert the raw dict from get_query_results() into a DataFrame.

    Real Athena duplicates column names as the first row in Rows — we detect and skip it.
    moto does NOT include that header row, so we use ResultSetMetadata.ColumnInfo instead.
    This helper works correctly with both.
    """
    rows = query_results["ResultSet"]["Rows"]
    col_info = query_results["ResultSet"].get("ResultSetMetadata", {}).get("ColumnInfo", [])

    if col_info:
        # Preferred path: get headers from metadata (works for both moto and real Athena)
        headers = [c["Name"] for c in col_info]
        # Real Athena duplicates headers as first row — detect and skip it
        if rows and [cell.get("VarCharValue", "") for cell in rows[0]["Data"]] == headers:
            rows = rows[1:]
    else:
        # Fallback: first row is the header (real Athena without injected results)
        if not rows:
            return pd.DataFrame()
        headers = [cell.get("VarCharValue", "") for cell in rows[0]["Data"]]
        rows = rows[1:]

    data = [[cell.get("VarCharValue", "") for cell in row["Data"]] for row in rows]
    return pd.DataFrame(data, columns=headers)


def list_all_objects(s3_client, bucket: str, prefix: str = "") -> list[dict]:
    """Return ALL objects under a prefix, handling pagination automatically."""
    objects = []
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        objects.extend(resp.get("Contents", []))
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break
    return objects
