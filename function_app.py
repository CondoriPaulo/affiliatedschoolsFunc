import azure.functions as func
import json
import re
import logging

app = func.FunctionApp()

# Lines that are column headers or page markers — skip them
SKIP_LINES = {
    "Name of School and State",
    "Operating units included on affiliation agreement listed below.",
    "Expiration date",
    "Programs",
}
PAGE_RE = re.compile(r"^Page \d+ of \d+$")
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")


def parse_schools(content: str) -> list:
    """
    Parse the flat text from Document Intelligence.

    The PDF repeats this 4-line block for every school:
        Line 0: School name
        Line 1: Operating units string
        Line 2: Expiration date  (MM/DD/YYYY)  ← used as anchor
        Line 3: Programs

    Strategy: scan forward; when lines[i+2] matches the date pattern
    then lines[i] is a school name. Advance by 4 and repeat.
    """
    lines = [ln.strip() for ln in content.split("\n") if ln.strip()]

    schools = []
    seen = set()  # deduplicate (the PDF lists Emory NHWSON twice)
    i = 0

    while i < len(lines):
        line = lines[i]

        # Skip known header / page-break lines
        if line in SKIP_LINES or PAGE_RE.match(line):
            i += 1
            continue

        # Look-ahead: need at least 3 more lines
        if i + 2 < len(lines) and DATE_RE.match(lines[i + 2]):
            school_name = re.sub(r"\s+", " ", line).strip()
            expiration_date = lines[i + 2]  # kept for logging; not in dim_school
            programs = lines[i + 3] if i + 3 < len(lines) else None

            logging.info(
                f"Found school: {school_name!r} | exp {expiration_date} | programs {programs!r}"
            )

            if school_name not in seen:
                seen.add(school_name)
                schools.append(
                    {
                        "school_name": school_name,
                        "tier": None,        # not in source PDF — assign manually in Power BI or SQL
                        "is_active": 1,
                    }
                )
            else:
                logging.warning(f"Duplicate school skipped: {school_name!r}")

            i += 4
        else:
            i += 1

    return schools


@app.function_name(name="ParseAffiliatedSchools")
@app.route(route="", methods=["POST"])
def parse_affiliated_schools(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger called by Logic App after Document Intelligence runs.

    Expected request body: the full Document Intelligence JSON
    (the Logic App passes the Analyze Document output directly).

    Returns:
        { "table": "dim_school", "rows": [ { school fields... }, ... ] }

    Logic App reads "table" to pick the SQL table and loops over "rows"
    to insert one record per iteration using the SQL "Insert Row" action.
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

    # Accept either the full Document Intelligence response or a plain
    # { "content": "..." } wrapper from a Compose step in Logic App
    content = body.get("content") or body.get("analyzeResult", {}).get("content", "")

    if not content:
        return func.HttpResponse(
            json.dumps({"error": "No 'content' field found in request body"}),
            status_code=400,
            mimetype="application/json",
        )

    rows = parse_schools(content)
    logging.info(f"Returning {len(rows)} dim_school rows.")

    return func.HttpResponse(
        json.dumps({"table": "dim_school", "rows": rows}, ensure_ascii=False),
        status_code=200,
        mimetype="application/json",
    )
