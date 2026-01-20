# FabricAgent skeleton

This repo contains a minimal Azure Functions backend for querying a Fabric Lakehouse SQL endpoint, plus metadata for routing.

## Layout
- `functions/query`: HTTP POST `/api/query` to run whitelisted SELECTs against the Fabric SQL endpoint.
- `metadata/tables.json`: LLM-oriented metadata for tables/columns/questions.
- `metadata/query_templates.json`: Query templates for routing.
- `metadata/router_rules.json`: Intent → template routing rules.

## Environment variables
Configure Function App settings:
- `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`: AAD app for database access.
- `FABRIC_SQL_SERVER`: e.g. `bo6vvxhe6jgu3hopjyevcqbkvm-pvcvibkzdncujaoltpo2yrhb64.datawarehouse.fabric.microsoft.com`
- `FABRIC_SQL_DATABASE`: `patient`

## Query API (draft)
`POST /api/query`
```json
{
	"table": "Vitals",
	"patient_id": "P001",
	"encounter_id": "E123",
	"from": "2025-12-01T00:00:00Z",
	"to": "2025-12-02T00:00:00Z",
	"latest": true,
	"limit": 50
}
```
- `table` must be one of: Patient, Encounter, Vitals, IntakeOutput, NursingNote, PhysicianProgressNote, WeeklySummary, LabResult, Medication, ImagingExam.
- Filters are optional; whitelisted columns only. Default limit 50, max 200. `latest=true` orders desc by the table datetime column.

Response:
```json
{ "table": "Vitals", "count": 3, "rows": [ {"PatientID": "P001", ...} ] }
```

## Run locally
```sh
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
func start
```
