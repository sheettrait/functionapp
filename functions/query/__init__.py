import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

import azure.functions as func
import pyodbc

SQL_COPT_SS_ACCESS_TOKEN = 1256

ALLOWED_TABLES: Dict[str, Dict[str, Any]] = {
    "Patient": {
        "columns": [
            "PatientID",
            "Sex",
            "DateOfBirth",
            "Age",
            "ChronicConditions",
            "RiskFlags",
            "City",
            "NationalID_Fake",
        ],
        "datetime_column": None,
        "order_by_fallback": "PatientID",
    },
    "Encounter": {
        "columns": [
            "EncounterID",
            "PatientID",
            "AdmissionDateTime",
            "DischargeDateTime",
            "Acuity",
            "Ward",
            "Room",
            "Bed",
            "AdmittingDiagnosis",
            "DischargeDisposition",
        ],
        "datetime_column": "AdmissionDateTime",
        "order_by_fallback": "AdmissionDateTime",
    },
    "Vitals": {
        "columns": [
            "VitalsID",
            "EncounterID",
            "PatientID",
            "DateTime",
            "Shift",
            "Temp_C",
            "HR_bpm",
            "BP_Sys_mmHg",
            "BP_Dia_mmHg",
            "RR_bpm",
            "SpO2_pct",
            "PainScore_0_10",
        ],
        "datetime_column": "DateTime",
        "order_by_fallback": "DateTime",
    },
    "IntakeOutput": {
        "columns": [
            "IO_ID",
            "EncounterID",
            "PatientID",
            "RecordStart",
            "RecordEnd",
            "Shift",
            "Intake_Oral_ml",
            "Intake_IV_ml",
            "Intake_TubeFeed_ml",
            "Output_Urine_ml",
            "Output_Drain_ml",
            "Output_Stool_count",
            "Output_Emesis_ml",
            "NetBalance_ml",
        ],
        "datetime_column": "RecordEnd",
        "order_by_fallback": "RecordEnd",
    },
    "NursingNote": {
        "columns": [
            "NursingNoteID",
            "EncounterID",
            "PatientID",
            "NoteDateTime",
            "Shift",
            "NoteType",
            "NurseID_Fake",
            "NoteText",
        ],
        "datetime_column": "NoteDateTime",
        "order_by_fallback": "NoteDateTime",
    },
    "PhysicianProgressNote": {
        "columns": [
            "PhysicianProgressNoteID",
            "EncounterID",
            "PatientID",
            "NoteDate",
            "ProviderID_Fake",
            "Service",
            "NoteText",
        ],
        "datetime_column": "NoteDate",
        "order_by_fallback": "NoteDate",
    },
    "WeeklySummary": {
        "columns": [
            "WeeklySummaryID",
            "EncounterID",
            "PatientID",
            "WeekStartDate",
            "WeekEndDate",
            "SummaryDate",
            "SummaryText",
        ],
        "datetime_column": "SummaryDate",
        "order_by_fallback": "SummaryDate",
    },
    "LabResult": {
        "columns": [
            "LabResultID",
            "EncounterID",
            "PatientID",
            "SpecimenDateTime",
            "LabType",
            "TestName",
            "ResultValue",
            "Unit",
            "Flag",
        ],
        "datetime_column": "SpecimenDateTime",
        "order_by_fallback": "SpecimenDateTime",
    },
    "Medication": {
        "columns": [
            "MedicationID",
            "EncounterID",
            "PatientID",
            "MedicationName",
            "Dose",
            "Route",
            "Frequency",
            "StartDateTime",
            "EndDateTime",
            "Indication",
        ],
        "datetime_column": "StartDateTime",
        "order_by_fallback": "StartDateTime",
    },
    "ImagingExam": {
        "columns": [
            "ImagingExamID",
            "EncounterID",
            "PatientID",
            "ExamDateTime",
            "ExamType",
            "Modality",
            "StudyName",
            "FindingText",
            "ImpressionText",
        ],
        "datetime_column": "ExamDateTime",
        "order_by_fallback": "ExamDateTime",
    },
}

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def get_connection() -> pyodbc.Connection:
    server = os.environ.get("FABRIC_SQL_SERVER")
    database = os.environ.get("FABRIC_SQL_DATABASE")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")

    if not all([server, database, client_id, client_secret]):
        raise RuntimeError("Missing required environment variables for Fabric SQL connection")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
        "Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={client_id};"
        f"PWD={client_secret};"
    )

    return pyodbc.connect(conn_str, autocommit=True)


def parse_iso_datetime(value: str) -> datetime:
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid datetime format: {value}") from exc


def build_query(table: str, body: Dict[str, Any]) -> Tuple[str, List[Any]]:
    meta = ALLOWED_TABLES[table]
    columns = meta["columns"]
    dt_col = meta["datetime_column"]
    order_by = dt_col or meta["order_by_fallback"]

    conditions: List[str] = []
    params: List[Any] = []

    patient_id = body.get("patient_id")
    encounter_id = body.get("encounter_id")
    from_ts = body.get("from")
    to_ts = body.get("to")
    shift = body.get("shift")
    latest = bool(body.get("latest", False))
    limit = body.get("limit", DEFAULT_LIMIT)
    try:
        limit = int(limit)
    except Exception:  # noqa: BLE001
        limit = DEFAULT_LIMIT
    limit = max(1, min(limit, MAX_LIMIT))

    if patient_id and "PatientID" in columns:
        conditions.append("PatientID = ?")
        params.append(patient_id)

    if encounter_id and "EncounterID" in columns:
        conditions.append("EncounterID = ?")
        params.append(encounter_id)

    if shift and "Shift" in columns:
        conditions.append("Shift = ?")
        params.append(shift)

    if from_ts and dt_col:
        conditions.append(f"{dt_col} >= ?")
        params.append(parse_iso_datetime(from_ts))

    if to_ts and dt_col:
        conditions.append(f"{dt_col} <= ?")
        params.append(parse_iso_datetime(to_ts))

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    order_clause = f" ORDER BY {order_by} DESC" if latest else f" ORDER BY {order_by} ASC"

    query = (
        f"SELECT {', '.join(columns)} FROM {table}"
        f"{where_clause}{order_clause} OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
    )
    params.append(limit)

    return query, params


def rows_to_dict(cursor: pyodbc.Cursor, rows: List[pyodbc.Row]) -> List[Dict[str, Any]]:
    col_names = [col[0] for col in cursor.description]
    return [dict(zip(col_names, row)) for row in rows]


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("query function processing a request")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    table = body.get("table")
    if not table or table not in ALLOWED_TABLES:
        return func.HttpResponse("Missing or unsupported table", status_code=400)

    try:
        query, params = build_query(table, body)
    except ValueError as exc:
        return func.HttpResponse(str(exc), status_code=400)

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
    except Exception as exc:  # noqa: BLE001
        logging.exception("Query execution failed")
        return func.HttpResponse(f"Query failed: {exc}", status_code=500)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    result = rows_to_dict(cursor, rows)
    return func.HttpResponse(
        json.dumps({"table": table, "count": len(result), "rows": result}, default=str),
        status_code=200,
        mimetype="application/json",
    )
