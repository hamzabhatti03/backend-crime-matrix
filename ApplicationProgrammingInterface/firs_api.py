from flask import Flask, request, jsonify, g
import sqlite3
from datetime import datetime

app = Flask(__name__)


def get_db_connection():
    if 'db_conn' not in g:
        g.db_conn = sqlite3.connect('../DatabaseManager/cms_staging.db')
        g.db_conn.row_factory = sqlite3.Row
    return g.db_conn


def get_leads_in_db_connection():
    if 'leads_conn' not in g:
        g.leads_conn = sqlite3.connect('../leads_in_all_districts1.db')
        g.leads_conn.row_factory = sqlite3.Row
    return g.leads_conn


# Close database connection after the request
@app.teardown_appcontext
def close_db_connection(exception=None):
    db_conn = g.pop('db_conn', None)
    leads_conn = g.pop('leads_conn', None)

    if db_conn is not None:
        db_conn.close()
    if leads_conn is not None:
        leads_conn.close()


def date_to_unix(date_str, is_start=True):
    date_format = '%Y-%m-%d %H:%M:%S'
    time_str = '00:00:00' if is_start else '23:59:59'
    dt = datetime.strptime(f'{date_str} {time_str}', date_format)
    return int(dt.timestamp())


@app.route('/get_dashboard_stats', methods=['GET'])
def get_dashboard_stats():
    from_date = request.args.get('fromDate')
    to_date = request.args.get('toDate')
    district = request.args.get('district')
    division = request.args.get('division')
    police_station = request.args.get('police_station')

    if not from_date or not to_date:
        return jsonify({"error": "Both fromDate and toDate are required"}), 400

    from_date_unix = date_to_unix(from_date, is_start=True)
    to_date_unix = date_to_unix(to_date, is_start=False)

    # Reuse database connections
    conn = get_db_connection()
    leads_conn = get_leads_in_db_connection()
    cursor = conn.cursor()
    leads_cursor = leads_conn.cursor()

    filters = []
    params = []

    if district:
        filters.append("district = ?")
        params.append(district)
    if division:
        filters.append("police_circle = ?")
        params.append(division)
    if police_station:
        filters.append("police_station = ?")
        params.append(police_station)

    filters.append("status = 'CompCa'")
    filters.append("level1_case_nature IN ('Crime Against Person','Crime Against Property')")
    filters.append("time_id BETWEEN ? AND ?")
    params.extend([from_date_unix, to_date_unix])

    where_clause = " AND ".join(filters)

    # Step 1: Get generated cases from leads_in table
    leads_in_query = f"""
        SELECT lead_id, level1_case_nature, level3_case_nature
        FROM leads_in
        WHERE {where_clause}
    """
    leads_cursor.execute(leads_in_query, params)
    leads = leads_cursor.fetchall()

    if not leads:
        return jsonify([])

    lead_ids = [str(lead["lead_id"]) for lead in leads]
    leads_by_case_nature = {}

    for lead in leads:
        case_nature = lead["level3_case_nature"]
        if case_nature not in leads_by_case_nature:
            leads_by_case_nature[case_nature] = {
                "lead_ids": [],
                "generated_cases": 0,
                "etags": 0,
                "firs_generated": 0,
                "category": ""
            }
        leads_by_case_nature[case_nature]["lead_ids"].append(lead["lead_id"])
        leads_by_case_nature[case_nature]["generated_cases"] += 1
        leads_by_case_nature[case_nature]["category"] = lead["level1_case_nature"]

    # Step 2: Get Etags and FIRs
    etags_and_firs_query = f"""
        SELECT
            prd.CaseId,
            SUM(CASE WHEN prd.ComplaintRecord IS NOT NULL THEN 1 ELSE 0 END) AS Etags,
            SUM(CASE WHEN cp.FIR_No IS NOT NULL THEN 1 ELSE 0 END) AS FIRs
        FROM pucar_15_raw_data prd
        LEFT JOIN complaints_pitb cp ON prd.ComplaintId = cp.Complaint_Id
        WHERE prd.CaseId IN ({','.join(['?' for _ in lead_ids])})
        GROUP BY prd.CaseId
    """
    cursor.execute(etags_and_firs_query, lead_ids)
    etags_and_firs = cursor.fetchall()

    etags_dict = {row['CaseId']: (row['Etags'], row['FIRs']) for row in etags_and_firs}

    # Step 3: Update leads_by_case_nature with Etags and FIRs
    for case_nature, data in leads_by_case_nature.items():
        for lead_id in data["lead_ids"]:
            etags, firs = etags_dict.get(lead_id, (0, 0))
            data["etags"] += etags
            data["firs_generated"] += firs
            # Ensure FIRs do not exceed generated cases
            if data["firs_generated"] > data["generated_cases"]:
                data["firs_generated"] = data["generated_cases"]

    final_response = []
    total_generated_cases = 0
    total_generated_etags = 0
    total_fir_generated = 0

    for case_nature, data in leads_by_case_nature.items():
        conversion_rate = (data["firs_generated"] / data["generated_cases"]) * 100 if data["generated_cases"] > 0 else 0
        total_generated_cases += data["generated_cases"]
        total_generated_etags += data["etags"]
        total_fir_generated += data["firs_generated"]

        final_response.append({
            "category": data["category"],
            "case_nature": case_nature,
            "generated_cases": data["generated_cases"],
            "etags_generated": int(data["etags"] / 2),
            "firs_generated": data["firs_generated"],
            "conversion_rate": f"{conversion_rate:.2f}%"
        })

    # Total summary
    avg_fir_conversion_rate = (total_fir_generated / total_generated_cases) * 100 if total_generated_cases > 0 else 0
    final_response.append({
        "category": "",
        "case_nature": "Total",
        "generated_cases": total_generated_cases,
        "etags_generated": int(total_generated_etags / 2),
        "firs_generated": total_fir_generated,
        "conversion_rate": f"{avg_fir_conversion_rate:.2f}"
    })

    return jsonify(final_response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5010)
