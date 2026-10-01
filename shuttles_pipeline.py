"""
Shuttles Dashboard — Daily Data Pipeline
=========================================
Queries Redshift (trips) + Salesforce (opportunities), pre-aggregates to 3 CSVs,
and uploads them to a Google Drive folder.

Run daily (cron, scheduled task, or manually):
    python shuttles_pipeline.py

Requirements:
    pip install redshift_connector simple_salesforce google-auth google-auth-httplib2 \
                google-api-python-client pandas python-dotenv

Environment variables (put in a .env file next to this script, never commit it):
    REDSHIFT_HOST
    REDSHIFT_PORT        (default 5439)
    REDSHIFT_DATABASE
    REDSHIFT_USER
    REDSHIFT_PASSWORD
    SF_USERNAME
    SF_PASSWORD
    SF_SECURITY_TOKEN    (from Salesforce: My Settings → Personal → Reset My Security Token)
    GOOGLE_SERVICE_ACCOUNT_JSON   (path to the service account key file — see setup notes)
    DRIVE_FOLDER_ID      (Google Drive folder ID — see setup notes)
"""

import os, io, json, logging
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

import pandas as pd
import redshift_connector
from simple_salesforce import Salesforce
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

REDSHIFT_HOST     = os.environ["REDSHIFT_HOST"]
REDSHIFT_PORT     = int(os.getenv("REDSHIFT_PORT", 5439))
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_USER     = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]

SF_USERNAME       = os.environ["SF_USERNAME"]
SF_PASSWORD       = os.environ["SF_PASSWORD"]
SF_SECURITY_TOKEN = os.environ["SF_SECURITY_TOKEN"]

GCP_KEY_FILE      = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
DRIVE_FOLDER_ID   = os.environ["DRIVE_FOLDER_ID"]
SETTINGS_SHEET_ID = os.environ.get("SETTINGS_SHEET_ID", "")  # same sheet used by Code.gs

# How far back to pull trip data (keep this generous so charts show full history)
TRIP_LOOKBACK_MONTHS = 30

# How far back to pull CLOSED opportunities (open ones are always included).
# 0 = all time. Funnel Metrics needs the full history; set e.g. OPP_LOOKBACK_MONTHS=60
# in the environment to limit it.
OPP_LOOKBACK_MONTHS = int(os.getenv("OPP_LOOKBACK_MONTHS", "0"))


# ── Redshift ────────────────────────────────────────────────────────────────────

def get_redshift_conn():
    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )


TRIPS_SQL = """
-- trips_monthly.csv
-- One row per (pickup_month, opportunity_id, product, trip_status, customer_account, currency,
--              organization, parent_id, investor_product_level_2).
-- Non-revenue fields: business_analytics.reservations_with_lawa (LAWA).
-- Revenue fields (per revenue SQL):
--   gross_revenue   = SUM(r.amount / conv_fact)          where conv_fact = cad_to_usd for CAD, else 1
--   referred_amount = SUM(r.referred_amount / conv_fact) same conversion
--   net_revenue     = SUM(bar.net_rev)                   from business_analytics.reservations
-- opportunity_id links to Salesforce: coachrail.quotes.opportunity_id = sf_opportunity.id.
-- Trips without an opportunity_id are included with NULL opportunity_id (marketplace trips).
-- overall_account and sf_account_name are added in Python after joining with Salesforce.

SELECT
    DATE_TRUNC('month', CONVERT_TIMEZONE('UTC','EST5EDT', lbar.pickup_date))::date  AS pickup_month,
    q.opportunity_id,
    lbar.product,
    lbar.new_res_status                     AS trip_status,
    ca.id                                   AS customer_account_id,
    ca.name                                 AS customer_account_name,
    ca.email                                AS customer_account_email,
    lbar.original_currency                  AS currency,
    lbar.organization,
    lbar.parent_id,
    lbar.investor_product_level_2,
    lbar.credited_rep,
    lbar.credited_manager,
    COUNT(*)                                AS trip_count,
    SUM(r.amount / CASE WHEN r.currency ILIKE '%CAD%'
                        THEN COALESCE(ccd.cad_to_usd, 1) ELSE 1 END)           AS gross_revenue,
    SUM(r.referred_amount / CASE WHEN r.currency ILIKE '%CAD%'
                                 THEN COALESCE(ccd.cad_to_usd, 1) ELSE 1 END)  AS referred_amount,
    SUM(bar.net_rev)                        AS net_revenue
FROM business_analytics.reservations_with_lawa lbar
JOIN coachrail.reservations r
       ON r.id = lbar.reservation_id
LEFT JOIN business_analytics.currency_conversion_daily ccd
       ON DATE(CONVERT_TIMEZONE('UTC','EST5EDT', r.pickup_date)) = ccd.pickup_date
LEFT JOIN business_analytics.reservations bar
       ON bar.reservation_id = lbar.reservation_id
LEFT JOIN coachrail.quotes q
       ON lbar.quote_id = q.id
LEFT JOIN coachrail.customers c
       ON q.customer_id = c.id
LEFT JOIN coachrail.customer_accounts ca
       ON c.customer_account_id = ca.id
WHERE lbar.new_res_status != 'hold'
  AND lbar.original_amount >= 0
  AND TRUNC(CONVERT_TIMEZONE('UTC','EST5EDT', lbar.pickup_date)) >= DATEADD(month, -{lookback}, CURRENT_DATE)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
ORDER BY 1, 2
""".format(lookback=TRIP_LOOKBACK_MONTHS)


def fetch_trips(conn) -> pd.DataFrame:
    log.info("Fetching trips from Redshift …")
    with conn.cursor() as cur:
        cur.execute(TRIPS_SQL)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df["pickup_month"] = pd.to_datetime(df["pickup_month"]).dt.strftime("%Y-%m")
    log.info(f"  {len(df):,} trip-month rows fetched")
    return df


# ── Salesforce ──────────────────────────────────────────────────────────────────

OPPORTUNITY_SOQL = """
SELECT
    Id,
    Name,
    AccountId,
    Account.Name,
    Account.ParentId,
    Account.Parent.Name,
    Owner.Name,
    Owner.Manager.Name,
    Type,
    StageName,
    Amount,
    Net_Revenue__c,
    CloseDate,
    CreatedDate,
    Product__c,
    Deal_Product_Type__c,
    Lead_Type__c,
    Start_Date__c,
    Duration_in_Months__c,
    LastModifiedDate,
    Lost_From_Stage__c,
    Total_Project_Cost_Input__c,
    Closed_Lost_Reason__c,
    Closed_Lost_Competitor__c,
    Closed_Lost_Competitor_Price__c,
    Effective_End_Date__c,
    Rep_Forecast_Category__c,
    ACV__c,
    Sourced_by_SDR__r.Name,
    Share_Credit_With__r.Name,
    Credit_To_Share__c,
    Account.ZI_Industry__c,
    Lead_Type_Detail__c,
    Requires_Formal_RFP_Process__c,
    Closed_Lost_Notes__c,
    Markup__c,
    IsWon,
    IsClosed
FROM Opportunity
{where}
ORDER BY CloseDate DESC
""".format(where=(
    "WHERE (IsClosed = false OR CloseDate >= LAST_N_MONTHS:%d)" % OPP_LOOKBACK_MONTHS
    if OPP_LOOKBACK_MONTHS > 0 else ""
))
# NOTE: Sourced_by_SDR__c and Share_Credit_With__c are assumed to be User lookup fields.
#       The query uses __r.Name to get the rep's display name rather than the 18-char User ID.
#       If either field is a plain text field in your org (not a lookup), replace
#       "Sourced_by_SDR__r.Name" with "Sourced_by_SDR__c" (and similarly for Share_Credit_With)
#       and update the fetch_opportunities() row-building code below accordingly.
# NOTE: Verify all custom field API names in your Salesforce org.

FIELD_HISTORY_SOQL = """
SELECT OpportunityId, Field, OldValue, NewValue, CreatedDate
FROM OpportunityFieldHistory
WHERE Field IN ('StageName', 'Amount', 'Total_Project_Cost_Input__c', 'CloseDate', 'Rep_Forecast_Category__c')
  AND CreatedDate >= LAST_N_MONTHS:{lookback}
ORDER BY OpportunityId, CreatedDate ASC
""".format(lookback=TRIP_LOOKBACK_MONTHS)
# NOTE: Field history tracking must be enabled in Salesforce Setup for
# Total_Project_Cost_Input__c, CloseDate, and Rep_Forecast_Category__c.
# If a field has no tracking enabled, OpportunityFieldHistory will return
# no rows for it and _field_value_at() will fall back to the current value.

# CUP_Account__c: links customer_accounts.email → Salesforce Account → Overall Account.
# Used to resolve overall_account for trips that have no opportunity_id (marketplace/direct).
CUP_ACCOUNT_SOQL = """
SELECT
    Email__c,
    Account__c,
    Account__r.Name,
    Account__r.Parent.Name
FROM CUP_Account__c
WHERE Email__c != null
"""

# Realized revenue per opportunity — uses LAWA for accounting-adjusted figures,
# same filters as TRIPS_SQL so numbers are consistent.
REALIZED_SQL = """
SELECT
    q.opportunity_id,
    SUM(lbar.accounting_gross_booking)   AS realized_gross,
    SUM(lbar.accounting_net_rev)         AS realized_net
FROM coachrail.reservations r
JOIN business_analytics.reservations_with_lawa lbar ON r.id = lbar.reservation_id
JOIN coachrail.quotes q ON r.quote_id = q.id
WHERE q.opportunity_id IS NOT NULL
  AND r.company_id = 2
  AND r.reservation_type = 0
  AND r.reservation_status != 'cancelled'
  AND r.reservation_status != 'hold'
  AND r.is_active = 1
  AND r.amount >= 0
GROUP BY q.opportunity_id
"""


def fetch_opportunities(sf: Salesforce, conn) -> pd.DataFrame:
    log.info("Fetching opportunities from Salesforce …")
    result = sf.query_all(OPPORTUNITY_SOQL)
    records = result["records"]
    log.info(f"  {len(records):,} opportunity records fetched")

    rows = []
    for r in records:
        acct_name   = (r.get("Account") or {}).get("Name", "")
        parent_name = ((r.get("Account") or {}).get("Parent") or {}).get("Name", "")
        overall     = parent_name if parent_name else acct_name

        # Sourced_by_SDR__c and Share_Credit_With__c are User lookup fields.
        # The SOQL fetches __r.Name (display name).  If your org stores these as
        # plain text fields instead, swap the .get("Sourced_by_SDR__r") calls for
        # r.get("Sourced_by_SDR__c", "") and update the SOQL accordingly.
        sourced_by_bdr    = (r.get("Sourced_by_SDR__r")    or {}).get("Name", "") or ""
        share_credit_with = (r.get("Share_Credit_With__r") or {}).get("Name", "") or ""
        credit_to_share   = r.get("Credit_To_Share__c") or ""

        rows.append({
            "opportunity_id":    r["Id"],
            "name":              r["Name"],
            "overall_account":   overall,
            "sf_account_name":   acct_name,
            "owner":             (r.get("Owner") or {}).get("Name", ""),
            "owner_manager":     ((r.get("Owner") or {}).get("Manager") or {}).get("Name", ""),
            "type":              r.get("Type", ""),
            "stage":             r.get("StageName", ""),
            "amount":            r.get("Amount") or 0,
            "net_revenue":       r.get("Net_Revenue__c") or 0,
            "close_date":        r.get("CloseDate", ""),
            "created_date":      (r.get("CreatedDate") or "")[:10],
            "product__c":        r.get("Product__c", ""),
            "deal_product_type": r.get("Deal_Product_Type__c", ""),
            "lead_type":          r.get("Lead_Type__c", ""),
            "start_date":         r.get("Start_Date__c", ""),
            "duration_months":    r.get("Duration_in_Months__c") or "",
            "last_modified_date":  (r.get("LastModifiedDate") or "")[:10],
            "lost_from_stage":     r.get("Lost_From_Stage__c") or "",
            "total_project_cost":  r.get("Total_Project_Cost_Input__c") or 0,
            "closed_lost_reason":  r.get("Closed_Lost_Reason__c") or "",
            "closed_lost_competitor": r.get("Closed_Lost_Competitor__c") or "",
            "closed_lost_comp_price":  r.get("Closed_Lost_Competitor_Price__c") or 0,
            "effective_end_date":      r.get("Effective_End_Date__c", "") or "",
            "rep_forecast_category":   r.get("Rep_Forecast_Category__c", "") or "",
            "acv":                     r.get("ACV__c") or 0,
            # Attribution fields
            "Sourced_by_SDR__c":    sourced_by_bdr,    # display name of the sourcing BDR/SDR
            "Share_Credit_With__c": share_credit_with,  # display name of the rep sharing credit
            "Credit_To_Share__c":   credit_to_share,    # percentage (e.g. 50 for 50%)
            # Pipeline Funnel Metrics fields ──────────────────────────────────
            "zi_industry":          (r.get("Account") or {}).get("ZI_Industry__c", "") or "",
            "lead_type_detail":     r.get("Lead_Type_Detail__c") or "",
            "requires_rfp":         "Yes" if r.get("Requires_Formal_RFP_Process__c") else "No",
            "closed_lost_notes":    r.get("Closed_Lost_Notes__c") or "",
            "markup_pct":           r.get("Markup__c") or 0,
            "is_won":               bool(r.get("IsWon", False)),
            "is_closed":            bool(r.get("IsClosed", False)),
        })

    opps = pd.DataFrame(rows)

    # Join realized revenue from Redshift
    log.info("  Joining realized revenue from Redshift …")
    with conn.cursor() as cur:
        cur.execute(REALIZED_SQL)
        cols = [d[0] for d in cur.description]
        real_rows = cur.fetchall()
    realized = pd.DataFrame(real_rows, columns=cols)
    opps = opps.merge(realized, on="opportunity_id", how="left")
    opps["realized_gross"] = opps["realized_gross"].fillna(0)
    opps["realized_net"]   = opps["realized_net"].fillna(0)

    log.info(f"  Done — {len(opps):,} opportunities")
    return opps


def _field_value_at(history, current_value, date_str, is_numeric=False):
    """Return field value at date_str using history list [{old, new, date}, ...]."""
    before = [h for h in history if h["date"] <= date_str]
    if not before:
        raw = history[0]["old"] if history else current_value
    else:
        raw = before[-1]["new"]
    if is_numeric:
        try: return float(raw or 0)
        except (TypeError, ValueError): return 0.0
    return raw if raw is not None else current_value


def fetch_field_history(sf: Salesforce) -> pd.DataFrame:
    """Query OpportunityFieldHistory for StageName and Amount changes."""
    log.info("Fetching OpportunityFieldHistory from Salesforce …")
    result = sf.query_all(FIELD_HISTORY_SOQL)
    records = result["records"]
    log.info(f"  {len(records):,} field history records fetched")
    rows = []
    for r in records:
        raw_dt = r.get("CreatedDate") or ""
        rows.append({
            "opportunity_id":  r.get("OpportunityId", ""),
            "field":           r.get("Field", ""),
            "old_value":       r.get("OldValue"),
            "new_value":       r.get("NewValue"),
            "created_date":    raw_dt[:10],       # date-only; used by build_pipeline_history
            "created_datetime": raw_dt,           # full ISO timestamp; used by build_stage_history
        })
    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["opportunity_id", "field", "old_value", "new_value",
                 "created_date", "created_datetime"]
    )
    log.info(f"  {len(df):,} history rows")
    return df


# Stage name → numeric map shared with the Pipeline Funnel Metrics dashboard.
STAGE_MAP = {
    "0. Prospecting":    0,
    "1. Identification": 1,
    "2. Qualification":  2,
    "3. Proposal":       3,
    "4. Alignment":      4,
    "5. Negotiation":    5,
    "7. Closed Won":     7,
    "9. Closed Lost":    9,
}


def build_stage_history(history_df: pd.DataFrame) -> pd.DataFrame:
    """Extract StageName transitions for the Pipeline Funnel Metrics dashboard.

    Returns one row per stage transition with columns:
      opportunity_id, old_stage (int), new_stage (int), edit_date (ISO datetime string)
    Rows where either old or new stage is unknown in STAGE_MAP are dropped.
    """
    log.info("Building stage history …")
    sn = history_df[history_df["field"] == "StageName"].copy()
    sn["old_stage"] = sn["old_value"].map(STAGE_MAP)
    sn["new_stage"] = sn["new_value"].map(STAGE_MAP)
    sn = sn.dropna(subset=["old_stage", "new_stage"])
    sn["old_stage"] = sn["old_stage"].astype(int)
    sn["new_stage"] = sn["new_stage"].astype(int)
    out = sn[["opportunity_id", "old_stage", "new_stage", "created_datetime"]].copy()
    out = out.rename(columns={"created_datetime": "edit_date"})
    out = out.sort_values(["opportunity_id", "edit_date"]).reset_index(drop=True)
    log.info(f"  {len(out):,} stage transition rows")
    return out


def build_pipeline_history(opps_df: pd.DataFrame, history_df: pd.DataFrame):
    """Build monthly pipeline snapshot rows for each opportunity from 2023-01 to 2027-12.

    Returns a tuple (grouped_df, detail_df):
      grouped_df  — aggregated by (month, stage, product__c); used for the chart.
                    Columns: month, stage, product__c, gross, net_revenue, deal_count.
      detail_df   — one row per (month, opportunity_id); used for the drill-down modal.
                    Columns: month, opportunity_id, stage, gross, net_revenue, tpc,
                             close_date, rep_forecast_category.

    Historical accuracy:
      - StageName:                 reconstructed from OpportunityFieldHistory ✓
      - Amount:                    reconstructed from OpportunityFieldHistory ✓
      - Total_Project_Cost_Input__c: reconstructed from OpportunityFieldHistory ✓
        (requires field history tracking enabled in Salesforce for this field;
         falls back to current value if no history rows exist)
      - CloseDate:                 reconstructed from OpportunityFieldHistory ✓
        (used to correctly place Closed Won/Lost deals in the month they closed)
      - Rep_Forecast_Category__c:  reconstructed from OpportunityFieldHistory ✓
    """
    import calendar

    log.info("Building pipeline history …")

    # ── Build per-opp field history lookup ─────────────────────────────────────
    stage_hist  = {}
    amount_hist = {}
    tpc_hist    = {}
    close_hist  = {}
    rep_fc_hist = {}
    if not history_df.empty:
        for _, row in history_df.iterrows():
            oid   = row["opportunity_id"]
            entry = {"old": row["old_value"], "new": row["new_value"], "date": row["created_date"]}
            field = row["field"]
            if   field == "StageName":                   stage_hist.setdefault(oid, []).append(entry)
            elif field == "Amount":                       amount_hist.setdefault(oid, []).append(entry)
            elif field == "Total_Project_Cost_Input__c":  tpc_hist.setdefault(oid, []).append(entry)
            elif field == "CloseDate":                    close_hist.setdefault(oid, []).append(entry)
            elif field == "Rep_Forecast_Category__c":     rep_fc_hist.setdefault(oid, []).append(entry)

    months_range = []
    for year in range(2023, 2028):
        for month in range(1, 13):
            months_range.append(f"{year}-{month:02d}")

    records = []
    for _, opp in opps_df.iterrows():
        oid            = opp["opportunity_id"]
        current_stage  = opp.get("stage", "") or ""
        current_amount = float(opp.get("amount", 0) or 0)
        current_tpc    = float(opp.get("total_project_cost", 0) or 0)
        current_close  = opp.get("close_date", "") or ""
        current_rep_fc = opp.get("rep_forecast_category", "") or ""
        created        = (opp.get("created_date", "") or "")[:7]  # YYYY-MM

        opp_stage_hist  = stage_hist.get(oid, [])
        opp_amount_hist = amount_hist.get(oid, [])
        opp_tpc_hist    = tpc_hist.get(oid, [])
        opp_close_hist  = close_hist.get(oid, [])
        opp_rep_fc_hist = rep_fc_hist.get(oid, [])

        for month_str in months_range:
            yr, mon  = map(int, month_str.split("-"))
            last_day = calendar.monthrange(yr, mon)[1]
            month_end = f"{month_str}-{last_day:02d}"

            # Skip if opp didn't exist yet
            if created and created > month_str:
                continue

            stage_at  = _field_value_at(opp_stage_hist,  current_stage,  month_end, is_numeric=False)
            amount_at = _field_value_at(opp_amount_hist, current_amount, month_end, is_numeric=True)
            tpc_at    = _field_value_at(opp_tpc_hist,    current_tpc,    month_end, is_numeric=True)
            # CloseDate history entries are stored as date strings (YYYY-MM-DD)
            close_at  = _field_value_at(opp_close_hist,  current_close,  month_end, is_numeric=False) or current_close
            rep_fc_at = _field_value_at(opp_rep_fc_hist, current_rep_fc, month_end, is_numeric=False)

            # For Closed Won/Lost: only include in the month they actually closed.
            # Use the historically-reconstructed CloseDate so old snapshots place
            # the deal in the correct month even if the close date was later changed.
            is_closed = "Closed Won" in (stage_at or "") or "Closed Lost" in (stage_at or "")
            if is_closed:
                close_month = (close_at or "")[:7]
                if close_month != month_str:
                    continue

            # Net Revenue = Amount − TPC (both at their historical values).
            # If TPC was 0 at snapshot time, fall back to 20% margin estimate.
            net_revenue = (amount_at - tpc_at) if tpc_at > 0 else (amount_at * 0.20)

            records.append({
                "month":                 month_str,
                "opportunity_id":        oid,
                "overall_account":       opp.get("overall_account", ""),
                "owner":                 opp.get("owner", ""),
                "stage":                 stage_at,
                "gross":                 amount_at,
                "net_revenue":           net_revenue,
                "tpc":                   tpc_at,
                "close_date":            close_at,
                "rep_forecast_category": rep_fc_at,
                "product__c":            opp.get("product__c", ""),
                "deal_product_type":     opp.get("deal_product_type", ""),
                "lead_type":             opp.get("lead_type", ""),
            })

    _EMPTY_GROUPED = pd.DataFrame(columns=[
        "month", "stage", "product__c", "gross", "net_revenue", "deal_count"
    ])
    _EMPTY_DETAIL = pd.DataFrame(columns=[
        "month", "opportunity_id", "stage", "gross", "net_revenue",
        "tpc", "close_date", "rep_forecast_category",
    ])

    if not records:
        log.info("  No pipeline history rows generated")
        return _EMPTY_GROUPED, _EMPTY_DETAIL

    df = pd.DataFrame(records)
    df = df[df["stage"].notna() & (df["stage"] != "")]  # drop null-stage rows

    # ── Per-deal detail CSV (for drill-down modal) ───────────────────────────────
    detail_df = df[[
        "month", "opportunity_id", "stage", "gross", "net_revenue",
        "tpc", "close_date", "rep_forecast_category",
    ]].copy()
    log.info(f"  {len(detail_df):,} pipeline history deal-rows (ungrouped)")

    # ── Grouped summary CSV (for chart) ─────────────────────────────────────────
    # Group by month + stage + product__c only — keeps the file small (~2-3k rows).
    grouped = (
        df.groupby(["month", "stage", "product__c"], dropna=False)
        .agg(
            gross=("gross", "sum"),
            net_revenue=("net_revenue", "sum"),
            deal_count=("opportunity_id", "nunique"),
        )
        .reset_index()
    )
    log.info(f"  {len(grouped):,} pipeline history rows (grouped)")
    return grouped, detail_df


SF_USERS_SOQL = """
SELECT Name, Manager.Name, CharterUP_Role__c
FROM User
WHERE IsActive = true
  AND UserType = 'Standard'
"""
# NOTE: Verify that 'CharterUP_Role__c' is the correct API field name in your SF org.
# This is used to identify sales roles (ISR, AE, CSP, Enterprise) in Pipeline Funnel Metrics.


ACTIVE_SF_USERS_SOQL = """
SELECT Id, Name, Username, CharterUP_Role__c, IsActive
FROM User
WHERE IsActive = true
ORDER BY Name
"""


def fetch_active_sf_users(sf: Salesforce) -> pd.DataFrame:
    """Active Salesforce users → active_sf_users.csv (drives "Active reps only" on Funnel Metrics)."""
    log.info("Fetching active Salesforce users for active_sf_users.csv …")
    records = sf.query_all(ACTIVE_SF_USERS_SOQL)["records"]
    rows = [{
        "Full Name":      r.get("Name", "") or "",
        "CharterUP Role": r.get("CharterUP_Role__c", "") or "",
        "Username":       r.get("Username", "") or "",
        "Active":         "TRUE" if r.get("IsActive") else "FALSE",
        "User ID":        r.get("Id", "") or "",
    } for r in records]
    df = pd.DataFrame(rows, columns=["Full Name", "CharterUP Role", "Username", "Active", "User ID"])
    log.info(f"  {len(df):,} active users")
    return df


def fetch_users(sf: Salesforce) -> pd.DataFrame:
    log.info("Fetching active SF users …")
    result = sf.query_all(SF_USERS_SOQL)
    records = result["records"]
    log.info(f"  {len(records):,} user records fetched")
    rows = []
    for r in records:
        mgr_name = ((r.get("Manager") or {}).get("Name") or "")
        rows.append({
            "user_name":      r.get("Name", ""),
            "manager_name":   mgr_name,
            "charterup_role": r.get("CharterUP_Role__c", "") or "",
        })
    df = pd.DataFrame(rows)
    df = df[df["user_name"].str.strip() != ""].reset_index(drop=True)
    log.info(f"  {len(df):,} users with names")
    return df


def fetch_cup_accounts(sf: Salesforce) -> dict:
    log.info("Fetching CUP_Account__c records from Salesforce …")
    result = sf.query_all(CUP_ACCOUNT_SOQL)
    records = result["records"]
    log.info(f"  {len(records):,} CUP_Account__c records fetched")
    lookup = {}
    for r in records:
        email = (r.get("Email__c") or "").strip().lower()
        if not email:
            continue
        acct_name   = (r.get("Account__r") or {}).get("Name", "") or ""
        parent_name = ((r.get("Account__r") or {}).get("Parent") or {}).get("Name", "") or ""
        overall     = parent_name if parent_name else acct_name
        if overall:
            lookup[email] = overall
    log.info(f"  {len(lookup):,} unique emails resolved to an Overall Account")
    return lookup


def build_accounts_ranked(trips_df: pd.DataFrame, opps_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building accounts ranked list …")
    trip_rev = (
        trips_df[trips_df["trip_status"] != "cancelled"]
        .groupby("overall_account", dropna=True)
        .agg(total_net_revenue=("net_revenue", "sum"))
        .reset_index()
    )
    all_accts = pd.DataFrame({
        "overall_account": pd.concat([
            trips_df["overall_account"].dropna(),
            opps_df["overall_account"].dropna(),
        ]).drop_duplicates()
    })
    ranked = (
        all_accts
        .merge(trip_rev, on="overall_account", how="left")
        .fillna({"total_net_revenue": 0})
        .sort_values("total_net_revenue", ascending=False)
        .reset_index(drop=True)
    )
    log.info(f"  {len(ranked)} total overall accounts")
    return ranked


# ── Forecast dataset ────────────────────────────────────────────────────────────

def _parse_date(s):
    if not s or (isinstance(s, float) and pd.isna(s)):
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _monthly_slices(start_d, end_d, gross_total, net_total):
    import calendar as _cal
    from datetime import date as _date
    if end_d < start_d:
        end_d = start_d
    total_days    = max((end_d - start_d).days + 1, 1)
    gross_per_day = gross_total / total_days
    net_per_day   = net_total   / total_days
    cur = _date(start_d.year, start_d.month, 1)
    while cur <= _date(end_d.year, end_d.month, 1):
        _, last_day = _cal.monthrange(cur.year, cur.month)
        month_start = max(start_d, cur)
        month_end   = min(end_d, _date(cur.year, cur.month, last_day))
        days        = (month_end - month_start).days + 1
        yield (
            cur.strftime("%Y-%m"),
            round(gross_per_day * days, 2),
            round(net_per_day   * days, 2),
        )
        cur = _date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)


def build_forecast(trips_df: pd.DataFrame, opps_df: pd.DataFrame,
                   users_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    import calendar as _cal
    from datetime import date as _date

    log.info("Building forecast dataset …")

    REP_FC_WEIGHTS, STAGE_WEIGHTS = load_forecast_weights()
    TRIP_STATUS_CAT = {
        "finished":  "Won - Finished",
        "completed": "Won - Finished",
        "started":   "Won - Started",
        "active":    "Won - Started",
        "upcoming":  "Won - Upcoming",
        "confirmed": "Won - Upcoming",
        "reserved":  "Won - Upcoming",
        "booked":    "Won - Upcoming",
    }

    opp_by_id = opps_df.set_index("opportunity_id").to_dict("index")

    mgr_first_to_full: dict = {}
    for _, opp in opps_df.iterrows():
        full = str(opp.get("owner_manager", "") or "").strip()
        if full and " " in full:
            first = full.split()[0].lower()
            mgr_first_to_full.setdefault(first, full)
    if users_df is not None and not users_df.empty:
        for mgr_full in users_df["manager_name"].dropna().unique():
            mgr_full = str(mgr_full).strip()
            if mgr_full and " " in mgr_full:
                first = mgr_full.split()[0].lower()
                mgr_first_to_full.setdefault(first, mgr_full)

    def _full_mgr(raw: str) -> str:
        raw = raw.strip()
        if raw and " " not in raw:
            return mgr_first_to_full.get(raw.lower(), raw)
        return raw

    from datetime import date as _today_date
    _today        = _today_date.today()
    _current_month = _today.strftime("%Y-%m")
    _prev_month = (
        f"{_today.year - 1}-12" if _today.month == 1
        else f"{_today.year}-{_today.month - 1:02d}"
    )

    def _opp_row(opp_id, month_str, gross, net, category, probability):
        opp = opp_by_id.get(opp_id, {})
        return {
            "pickup_month":             month_str,
            "opportunity_id":           opp_id,
            "product":                  opp.get("product__c", ""),
            "trip_status":              "",
            "customer_account_id":      "",
            "customer_account_name":    "",
            "customer_account_email":   "",
            "currency":                 "",
            "organization":             "",
            "parent_id":                "",
            "investor_product_level_2": "",
            "credited_rep":             opp.get("owner", ""),
            "credited_manager":         opp.get("owner_manager", ""),
            "trip_count":               "",
            "gross_revenue":            round(gross, 2),
            "referred_amount":          "",
            "net_revenue":              round(net, 2),
            "overall_account":          opp.get("overall_account", ""),
            "sf_account_name":          opp.get("sf_account_name", ""),
            "forecast_category":        category,
            "probability":              probability,
            "weighted_gross":           round(gross * probability, 2),
            "weighted_net":             round(net   * probability, 2),
            "Sourced_by_SDR__c":        opp.get("Sourced_by_SDR__c", "") or "",
            "Share_Credit_With__c":     opp.get("Share_Credit_With__c", "") or "",
            "Credit_To_Share__c":       opp.get("Credit_To_Share__c", "") or "",
        }

    rows = []

    _upcoming_statuses = {"upcoming", "confirmed", "reserved", "booked"}
    # For each opp, store the net/gross margin from its most recent finalized
    # (non-upcoming, has referred_amount) trip month.  Used to estimate net
    # revenue for upcoming months that lack a referred_amount.
    opp_margin: dict = {}
    valid_opp_mask = trips_df["opportunity_id"].notna()
    for oid, grp in trips_df[valid_opp_mask].groupby("opportunity_id"):
        def _real_ref(x):
            try:
                return float(x or 0) > 0
            except (TypeError, ValueError):
                return False
        finalized = grp[
            ~grp["trip_status"].str.lower().str.strip().isin(_upcoming_statuses) &
            grp["referred_amount"].apply(_real_ref)
        ]
        if finalized.empty:
            continue
        latest     = finalized.sort_values("pickup_month", ascending=False).iloc[0]
        last_net   = float(latest.get("net_revenue",   0) or 0)
        last_gross = float(latest.get("gross_revenue", 0) or 0)
        if last_gross > 0:
            opp_margin[str(oid)] = last_net / last_gross

    # ── Component 1: Won – Coachrail Trips ────────────────────────────────────
    for _, t in trips_df.iterrows():
        raw_status = str(t.get("trip_status", "") or "").strip()
        if raw_status.lower() in ("cancelled", "canceled"):
            category = "Won - Cancelled"
        else:
            category = TRIP_STATUS_CAT.get(raw_status.lower(), f"Won - {raw_status.title()}")
        gross      = float(t.get("gross_revenue", 0) or 0)
        net        = float(t.get("net_revenue",   0) or 0)

        ref_amt = float(t.get("referred_amount", 0) or 0)
        if ref_amt == 0 and category == "Won - Upcoming":
            oid_str = str(t.get("opportunity_id", "") or "")
            # Use the existing net if it looks like a real value: non-zero and
            # not equal to gross (net == gross suggests a Coachrail placeholder,
            # not a settled referral amount).  Fall back to the per-trip rate
            # estimate from the last finalized month otherwise.
            use_existing_net = net > 0 and net != gross
            if not use_existing_net and oid_str in opp_margin:
                net = round(gross * opp_margin[oid_str], 2)

        raw_mgr    = str(t.get("credited_manager", "") or "").strip()
        _trip_opp  = opp_by_id.get(str(t.get("opportunity_id", "") or ""), {})
        rows.append({
            "pickup_month":             str(t["pickup_month"]),
            "opportunity_id":           t.get("opportunity_id") or "",
            "product":                  t.get("product", ""),
            "trip_status":              raw_status,
            "customer_account_id":      t.get("customer_account_id", ""),
            "customer_account_name":    t.get("customer_account_name", ""),
            "customer_account_email":   t.get("customer_account_email", ""),
            "currency":                 t.get("currency", ""),
            "organization":             t.get("organization", ""),
            "parent_id":                t.get("parent_id", ""),
            "investor_product_level_2": t.get("investor_product_level_2", ""),
            "credited_rep":             t.get("credited_rep", ""),
            "credited_manager":         _full_mgr(raw_mgr),
            "trip_count":               t.get("trip_count", ""),
            "gross_revenue":            gross,
            "referred_amount":          t.get("referred_amount", ""),
            "net_revenue":              net,
            "overall_account":          t.get("overall_account", ""),
            "sf_account_name":          t.get("sf_account_name", ""),
            "forecast_category":        category,
            "probability":              1.0,
            "weighted_gross":           round(gross, 2),
            "weighted_net":             round(net,   2),
            "Sourced_by_SDR__c":        _trip_opp.get("Sourced_by_SDR__c", "") or "",
            "Share_Credit_With__c":     _trip_opp.get("Share_Credit_With__c", "") or "",
            "Credit_To_Share__c":       _trip_opp.get("Credit_To_Share__c", "") or "",
        })

    log.info(f"  Component 1 (Coachrail trips):   {len(rows):,} rows")
    c1_len = len(rows)

    # ── Component 2: Won – Synthetic Trips ────────────────────────────────────
    coachrail_coverage: dict = {}
    for _, t in trips_df[trips_df["opportunity_id"].notna()].iterrows():
        oid   = str(t["opportunity_id"])
        month = str(t["pickup_month"])
        g = float(t.get("gross_revenue", 0) or 0)
        n = float(t.get("net_revenue",   0) or 0)
        coachrail_coverage.setdefault(oid, {})
        prev = coachrail_coverage[oid].get(month, (0.0, 0.0))
        coachrail_coverage[oid][month] = (prev[0] + g, prev[1] + n)

    won_opps = opps_df[opps_df["stage"].str.contains("Closed Won", na=False)]
    log.info(f"  Won opps for synthetic trips:    {len(won_opps):,}")

    for _, opp in won_opps.iterrows():
        opp_id  = opp["opportunity_id"]
        start_d = _parse_date(opp.get("start_date"))
        end_d   = _parse_date(opp.get("effective_end_date"))
        gross   = float(opp.get("amount",      0) or 0)
        net     = float(opp.get("net_revenue", 0) or 0)

        if not start_d:
            continue
        if not end_d or end_d < start_d:
            end_d = start_d

        covered = coachrail_coverage.get(opp_id, {})

        term_months: set = set()
        cur = _date(start_d.year, start_d.month, 1)
        end_first = _date(end_d.year, end_d.month, 1)
        while cur <= end_first:
            term_months.add(cur.strftime("%Y-%m"))
            cur = _date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)

        missing = term_months - set(covered.keys())
        if not missing:
            continue

        if not covered:
            start_month = start_d.strftime("%Y-%m")
            if start_month < _current_month:
                continue
            for month_str, month_gross, month_net in _monthly_slices(start_d, end_d, gross, net):
                if month_str in missing:
                    if month_str < _current_month:
                        continue
                    rows.append(_opp_row(opp_id, month_str, month_gross, month_net,
                                         "Won - Pending", 1.0))
        else:
            last_month_str       = max(covered.keys())
            start_month          = start_d.strftime("%Y-%m")
            if start_month < _current_month and last_month_str < _prev_month:
                continue
            last_gross, last_net = covered[last_month_str]
            last_m_d = datetime.strptime(last_month_str, "%Y-%m").date()
            _, days_in_last_month = _cal.monthrange(last_m_d.year, last_m_d.month)
            for month_str in sorted(missing):
                if month_str <= last_month_str:
                    continue
                if month_str < _current_month:
                    continue
                month_d = datetime.strptime(month_str, "%Y-%m").date()
                _, days_in_month = _cal.monthrange(month_d.year, month_d.month)
                proration = days_in_month / days_in_last_month
                if month_d.year == end_d.year and month_d.month == end_d.month:
                    proration *= end_d.day / days_in_month
                rows.append(_opp_row(opp_id, month_str,
                                     round(last_gross * proration, 2),
                                     round(last_net   * proration, 2),
                                     "Won - Inferred", 1.0))

    log.info(f"  Component 2 (Synthetic Won):     {len(rows) - c1_len:,} rows")
    c2_len = len(rows)

    # ── Component 3: Open Deals ───────────────────────────────────────────────
    open_opps = opps_df[
        ~opps_df["stage"].str.contains("Closed Won",  na=False) &
        ~opps_df["stage"].str.contains("Closed Lost", na=False)
    ]
    log.info(f"  Open opps for forecast:          {len(open_opps):,}")

    for _, opp in open_opps.iterrows():
        net = float(opp.get("net_revenue", 0) or 0)
        if net < 0:
            continue

        gross   = float(opp.get("amount", 0) or 0)
        start_d = _parse_date(opp.get("start_date"))
        end_d   = _parse_date(opp.get("effective_end_date"))

        if not start_d:
            continue
        if not end_d or end_d < start_d:
            end_d = start_d

        rep_fc = str(opp.get("rep_forecast_category", "") or "").strip()
        stage  = str(opp.get("stage", "") or "").strip()

        if rep_fc in REP_FC_WEIGHTS:
            probability = REP_FC_WEIGHTS[rep_fc]
            category    = f"Open - Rep Forecast: {rep_fc}"
        else:
            probability = STAGE_WEIGHTS.get(stage, 0.0)
            category    = "Open - Stage-Weighted"

        for month_str, month_gross, month_net in _monthly_slices(start_d, end_d, gross, net):
            if month_str < _current_month:
                continue
            rows.append(_opp_row(opp["opportunity_id"], month_str,
                                 month_gross, month_net, category, probability))

    log.info(f"  Component 3 (Open Deals):        {len(rows) - c2_len:,} rows")

    if not rows:
        return pd.DataFrame(columns=[
            "pickup_month", "opportunity_id", "product", "trip_status",
            "customer_account_id", "customer_account_name", "customer_account_email",
            "currency", "organization", "parent_id", "investor_product_level_2",
            "credited_rep", "credited_manager", "trip_count",
            "gross_revenue", "referred_amount", "net_revenue",
            "overall_account", "sf_account_name",
            "forecast_category", "probability", "weighted_gross", "weighted_net",
        ])

    df = pd.DataFrame(rows)
    log.info(f"  Total forecast rows:             {len(df):,}")
    return df


# ── Google Sheets ──────────────────────────────────────────────────────────────

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GCP_KEY_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=creds)


# ── Forecast Weights (Settings sheet) ─────────────────────────────────────────

# Default weights — used when the Settings sheet is unavailable or the tab is missing.
_DEFAULT_REP_FC_WEIGHTS: dict = {"High": 0.90, "Medium": 0.50, "Low": 0.20}
_DEFAULT_STAGE_WEIGHTS: dict = {
    "0. Prospecting":    0.00,
    "1. Identification": 0.00,
    "2. Qualification":  0.05,
    "3. Proposal":       0.20,
    "4. Alignment":      0.50,
    "5. Negotiation":    0.85,
}


def load_forecast_weights() -> tuple:
    """Read the 'Forecast Weights' tab from the Settings Google Sheet.

    Tab layout (columns A–B, row 1 = header):
        Category               | Weight
        Rep Forecast: High     | 0.90
        Rep Forecast: Medium   | 0.50
        Rep Forecast: Low      | 0.20
        Stage: 0. Prospecting  | 0.00
        Stage: 2. Qualification| 0.05
        …

    Returns (rep_fc_weights, stage_weights) — falls back to hardcoded defaults
    if the sheet is unavailable, the tab is missing, or a row can't be parsed.
    """
    if not SETTINGS_SHEET_ID:
        log.info("SETTINGS_SHEET_ID not set — using default forecast weights")
        return _DEFAULT_REP_FC_WEIGHTS.copy(), _DEFAULT_STAGE_WEIGHTS.copy()

    try:
        svc    = get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SETTINGS_SHEET_ID,
            range="Forecast Weights!A:B",
        ).execute()
        rows = result.get("values", [])
        if not rows or len(rows) < 2:
            log.warning("'Forecast Weights' tab is empty — using default weights")
            return _DEFAULT_REP_FC_WEIGHTS.copy(), _DEFAULT_STAGE_WEIGHTS.copy()

        headers  = [h.strip().lower() for h in rows[0]]
        cat_idx  = headers.index("category") if "category" in headers else 0
        wt_idx   = headers.index("weight")   if "weight"   in headers else 1

        rep_fc: dict = {}
        stage:  dict = {}
        for row in rows[1:]:
            if len(row) <= max(cat_idx, wt_idx):
                continue
            cat = str(row[cat_idx]).strip()
            try:
                wt = float(row[wt_idx])
            except (ValueError, TypeError):
                continue
            if cat.lower().startswith("rep forecast:"):
                key = cat[len("rep forecast:"):].strip()
                rep_fc[key] = wt
            elif cat.lower().startswith("stage:"):
                key = cat[len("stage:"):].strip()
                stage[key] = wt

        if not rep_fc:
            log.warning("No 'Rep Forecast:' rows found — using default rep weights")
            rep_fc = _DEFAULT_REP_FC_WEIGHTS.copy()
        if not stage:
            log.warning("No 'Stage:' rows found — using default stage weights")
            stage = _DEFAULT_STAGE_WEIGHTS.copy()

        log.info(f"Loaded forecast weights: {len(rep_fc)} rep categories, {len(stage)} stages")
        return rep_fc, stage

    except Exception as exc:
        log.warning(f"Could not load Forecast Weights from Settings sheet ({exc}) — using defaults")
        return _DEFAULT_REP_FC_WEIGHTS.copy(), _DEFAULT_STAGE_WEIGHTS.copy()


# ── Table Sorts (Settings sheet) ───────────────────────────────────────────────

# Default sorts — match the hardcoded state2 defaults in the dashboard.
# dir: 1 = ascending, -1 = descending (same convention as the dashboard's state2).
_DEFAULT_TABLE_SORTS: dict = {
    "recentCreated": {"col": "netRevenue", "dir": -1},
    "closingSoon":   {"col": "netRevenue", "dir": -1},
    "recentLosses":  {"col": "netRevenue", "dir": -1},
    "stageReach":    {"col": "closeDate",  "dir":  1},
    "openDeals":     {"col": "netRevenue", "dir": -1},
    "recentWins":    {"col": "netRevenue", "dir": -1},
}

# Valid column names accepted in column B of the Table Sorts tab.
_VALID_SORT_COLS = {
    "netRevenue", "amount", "closeDate", "createdDate", "startDate",
    "name", "owner", "stage", "lastModifiedDate", "durationMonths",
    "lostFromStage", "age",
}


def load_table_sorts() -> dict:
    """Read the 'Table Sorts' tab from the Settings Google Sheet."""
    if not SETTINGS_SHEET_ID:
        log.info("SETTINGS_SHEET_ID not set — using default table sorts")
        return {k: dict(v) for k, v in _DEFAULT_TABLE_SORTS.items()}

    try:
        svc    = get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SETTINGS_SHEET_ID,
            range="Table Sorts!A:C",
        ).execute()
        rows = result.get("values", [])
        if not rows or len(rows) < 2:
            log.warning("'Table Sorts' tab is empty — using default sorts")
            return {k: dict(v) for k, v in _DEFAULT_TABLE_SORTS.items()}

        dir_map = {
            "asc": 1, "ascending": 1,
            "desc": -1, "descending": -1,
        }

        sorts = {k: dict(v) for k, v in _DEFAULT_TABLE_SORTS.items()}
        for row in rows[1:]:
            if len(row) < 3 or not row[0].strip():
                continue
            table     = row[0].strip()
            sort_col  = row[1].strip()
            direction = row[2].strip().lower()

            if table not in _DEFAULT_TABLE_SORTS:
                log.warning(f"  Table Sorts: unknown table key '{table}' — skipped")
                continue
            if sort_col not in _VALID_SORT_COLS:
                log.warning(f"  Table Sorts: unknown sort column '{sort_col}' for '{table}' — skipped")
                continue
            if direction not in dir_map:
                log.warning(f"  Table Sorts: unknown direction '{direction}' for '{table}' — skipped")
                continue

            sorts[table] = {"col": sort_col, "dir": dir_map[direction]}

        log.info(f"Loaded table sorts: {sorts}")
        return sorts

    except Exception as exc:
        log.warning(f"Could not load Table Sorts from Settings sheet ({exc}) — using defaults")
        return {k: dict(v) for k, v in _DEFAULT_TABLE_SORTS.items()}


# ── Config builder ─────────────────────────────────────────────────────────────

def build_config() -> dict:
    """Assemble the config dict that Code.gs will inject as SHUTTLE_DATA.config."""
    log.info("Building config from Settings sheet …")
    rep_fc_weights, stage_weights = load_forecast_weights()
    table_sorts = load_table_sorts()

    config = {
        "forecastWeights": {
            "repForecast": rep_fc_weights,
            "stage":       stage_weights,
        },
        "tableSorts": table_sorts,
    }
    log.info("Config ready")
    return config


# ── Google Drive upload ─────────────────────────────────────────────────────────

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        GCP_KEY_FILE,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def upsert_csv(service, folder_id: str, filename: str, df: pd.DataFrame):
    """Upload df as CSV to Drive folder, overwriting any existing file with the same name."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv", resumable=False)

    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute().get("files", [])

    if existing:
        file_id = existing[0]["id"]
        service.files().update(
            fileId=file_id, media_body=media, supportsAllDrives=True,
        ).execute()
        log.info(f"  Updated {filename} ({len(csv_bytes):,} bytes)")
    else:
        meta = {"name": filename, "parents": [folder_id]}
        service.files().create(
            body=meta, media_body=media, fields="id", supportsAllDrives=True,
        ).execute()
        log.info(f"  Created {filename} ({len(csv_bytes):,} bytes)")


def upsert_json(service, folder_id: str, filename: str, data: dict):
    """Upload a dict as JSON to Drive folder, overwriting any existing file with the same name."""
    json_bytes = json.dumps(data, indent=2).encode("utf-8")
    media = MediaIoBaseUpload(io.BytesIO(json_bytes), mimetype="application/json", resumable=False)

    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute().get("files", [])

    if existing:
        file_id = existing[0]["id"]
        service.files().update(
            fileId=file_id, media_body=media, supportsAllDrives=True,
        ).execute()
        log.info(f"  Updated {filename} ({len(json_bytes):,} bytes)")
    else:
        meta = {"name": filename, "parents": [folder_id]}
        service.files().create(
            body=meta, media_body=media, fields="id", supportsAllDrives=True,
        ).execute()
        log.info(f"  Created {filename} ({len(json_bytes):,} bytes)")


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Shuttles Dashboard Pipeline ===")
    started = datetime.utcnow()

    conn   = get_redshift_conn()
    sf     = Salesforce(username=SF_USERNAME, password=SF_PASSWORD,
                        security_token=SF_SECURITY_TOKEN)
    drive  = get_drive_service()

    trips            = fetch_trips(conn)
    opps             = fetch_opportunities(sf, conn)
    cup_lookup       = fetch_cup_accounts(sf)
    users            = fetch_users(sf)
    active_sf_users  = fetch_active_sf_users(sf)
    field_history_df = fetch_field_history(sf)

    # Join CharterUP Role onto opportunities by owner name.
    role_map = users.set_index("user_name")["charterup_role"].to_dict()
    opps["charterup_role"] = opps["owner"].map(role_map).fillna("")

    # Build stage history CSV for Pipeline Funnel Metrics tab.
    stage_history_df = build_stage_history(field_history_df)

    # Enrich trips with Salesforce account names.
    opp_accounts = (
        opps[["opportunity_id", "overall_account", "sf_account_name"]]
        .drop_duplicates(subset=["opportunity_id"])
    )
    trips = trips.merge(opp_accounts, on="opportunity_id", how="left")

    def _resolve_account(row):
        oa = row.get("overall_account")
        if oa and pd.notna(oa):
            return oa
        raw_email = row.get("customer_account_email")
        email = "" if (raw_email is None or (isinstance(raw_email, float) and pd.isna(raw_email))) else str(raw_email).strip().lower()
        return cup_lookup.get(email)

    trips["overall_account"] = trips.apply(_resolve_account, axis=1)

    accts        = build_accounts_ranked(trips, opps)
    pipe_hist_df, pipe_hist_deals_df = build_pipeline_history(opps, field_history_df)
    forecast_df  = build_forecast(trips, opps, users)

    config = build_config()

    log.info("Uploading files to Google Drive …")
    upsert_csv(drive,  DRIVE_FOLDER_ID, "trips_monthly.csv",    trips)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "opportunities.csv",    opps)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "stage_history.csv",    stage_history_df)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "accounts_ranked.csv",  accts)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "users.csv",            users)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "pipeline_history.csv",        pipe_hist_df)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "pipeline_history_deals.csv",  pipe_hist_deals_df)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "forecast_monthly.csv", forecast_df)
    upsert_csv(drive,  DRIVE_FOLDER_ID, "active_sf_users.csv",  active_sf_users)
    upsert_json(drive, DRIVE_FOLDER_ID, "config.json",          config)

    conn.close()
    elapsed = (datetime.utcnow() - started).total_seconds()
    log.info(f"=== Done in {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()
