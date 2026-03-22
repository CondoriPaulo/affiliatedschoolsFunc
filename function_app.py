import azure.functions as func
import json
import logging
from datetime import datetime

app = func.FunctionApp()


def parse_schools(analyze_result: dict) -> list:
    """
    Parse school rows from Document Intelligence structured table output.

    The PDF has 2 tables (across 3 pages). Each table has 4 columns:
        col 0: school_name
        col 1: operating_units
        col 2: expiration_date  (MM/DD/YYYY)
        col 3: programs

    Strategy: group cells by rowIndex, skip header rows (kind="columnHeader"),
    build one dict per data row. This avoids fragile regex on flat text and
    uses the structured output Document Intelligence already provides.
    """
    tables = analyze_result.get("tables", [])
    schools = []
    seen = set()

    for table_idx, table in enumerate(tables):
        # Group cells by rowIndex → { rowIndex: { colIndex: content } }
        rows = {}
        for cell in table.get("cells", []):
            if cell.get("kind") == "columnHeader":
                continue                          # skip header row
            r = cell["rowIndex"]
            c = cell["columnIndex"]
            rows.setdefault(r, {})[c] = cell.get("content", "").strip()

        logging.info(f"Table {table_idx}: {len(rows)} data rows found.")

        for row_idx in sorted(rows):
            cols = rows[row_idx]
            school_name = cols.get(0, "").strip()
            if not school_name:
                continue

            if school_name in seen:
                logging.warning(f"Duplicate school skipped: {school_name!r}")
                continue
            seen.add(school_name)

            # Convert MM/DD/YYYY → YYYY-MM-DD for SQL DATE column
            raw_date = cols.get(2, "")
            try:
                exp_date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
            except ValueError:
                exp_date = None
                logging.warning(f"Could not parse date {raw_date!r} for {school_name!r}")

            schools.append({
                "school_name":    school_name,
                "operating_units": cols.get(1) or None,
                "expiration_date": exp_date,
                "programs":        cols.get(3) or None,
                "tier":            None,   # not in PDF — assign manually in SQL later
                "is_active":       1,
            })
            logging.info(
                f"  → {school_name!r} | units={cols.get(1)!r} | exp={exp_date} | programs={cols.get(3)!r}"
            )

    return schools


@app.function_name(name="ParseAffiliatedSchools")
@app.route(route="", methods=["POST"])
def parse_affiliated_schools(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger called by Logic App after Document Intelligence runs.

    Expected body: the full Document Intelligence JSON response.
    Logic App passes it directly from the 'Analyze Document' action output.

    Returns:
        { "table": "dim_school", "rows": [ { school fields... }, ... ] }

    Logic App reads "table" to pick the SQL table, then loops over "rows"
    to insert one record per school using the SQL connector Insert Row action.
    """
    logging.info("ParseAffiliatedSchools triggered.")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Request body must be valid JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    # Logic App passes the full Document Intelligence response.
    # Handle both: raw DI response (has "analyzeResult") and plain wrapper.
    analyze_result = body.get("analyzeResult", body)

    if not analyze_result.get("tables"):
        return func.HttpResponse(
            json.dumps({"error": "No 'tables' found in analyzeResult. Ensure Document Intelligence used prebuilt-layout model."}),
            status_code=400,
            mimetype="application/json",
        )

    rows = parse_schools(analyze_result)
    logging.info(f"Returning {len(rows)} dim_school rows.")

    return func.HttpResponse(
        json.dumps({"table": "dim_school", "rows": rows}, ensure_ascii=False),
        status_code=200,
        mimetype="application/json",
    )
