import os
import sys
from typing import Any, Dict

import pyodbc
from azure.identity import ClientSecretCredential

SQL_COPT_SS_ACCESS_TOKEN = 1256

ALLOWED_TABLES = {
    "Patient",
    "Encounter",
    "Vitals",
    "IntakeOutput",
    "NursingNote",
    "PhysicianProgressNote",
    "WeeklySummary",
    "LabResult",
    "Medication",
    "ImagingExam",
}


def get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def get_connection() -> pyodbc.Connection:
    tenant_id = get_env("AZURE_TENANT_ID")
    client_id = get_env("AZURE_CLIENT_ID")
    client_secret = get_env("AZURE_CLIENT_SECRET")
    server = get_env("FABRIC_SQL_SERVER")
    database = get_env("FABRIC_SQL_DATABASE")

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    token = credential.get_token("https://database.windows.net/.default").token
    token_bytes = token.encode("utf-16-le")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_bytes}, autocommit=True)


def fetch_one_row(table: str) -> Dict[str, Any]:
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported table: {table}")

    query = f"SELECT TOP (1) * FROM {table}"

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            return {"table": table, "row": None}
        columns = [col[0] for col in cursor.description]
        return {"table": table, "row": dict(zip(columns, row))}


def main() -> None:
    table = os.environ.get("TEST_TABLE", "Patient")
    result = fetch_one_row(table)
    print(result)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}")
        sys.exit(1)
