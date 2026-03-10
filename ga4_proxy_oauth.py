from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import os
import json

app = FastAPI()

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
GOOGLE_ACCESS_TOKEN = os.getenv("GOOGLE_ACCESS_TOKEN")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")


# =============================
# OAuth
# =============================

def refresh_access_token():
    if not GOOGLE_REFRESH_TOKEN or not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise Exception("Google OAuth environment variables are not set")

    url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }

    r = requests.post(url, data=payload, timeout=60)

    if r.status_code != 200:
        raise Exception(f"Token refresh failed: {r.text}")

    return r.json()["access_token"]


def get_access_token():
    if GOOGLE_ACCESS_TOKEN:
        return GOOGLE_ACCESS_TOKEN
    return refresh_access_token()


# =============================
# GA4 Core
# =============================

def call_ga4(data: dict):
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

    access_token = get_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{GA4_PROPERTY_ID}:runReport"

    r = requests.post(url, headers=headers, json=data, timeout=120)

    if r.status_code == 401:
        access_token = refresh_access_token()
        headers["Authorization"] = f"Bearer {access_token}"
        r = requests.post(url, headers=headers, json=data, timeout=120)

    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    return r.json()


# =============================
# BigQuery Core
# =============================

def get_bq_client():
    if not BIGQUERY_PROJECT_ID:
        raise HTTPException(status_code=500, detail="BIGQUERY_PROJECT_ID not set")

    if not BIGQUERY_DATASET:
        raise HTTPException(status_code=500, detail="BIGQUERY_DATASET not set")

    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise HTTPException(status_code=500, detail="GOOGLE_SERVICE_ACCOUNT_JSON not set")

    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID, credentials=credentials)
        return client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery client init failed: {str(e)}")


def run_bq_query(sql: str, params: list):
    client = get_bq_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=params
    )

    try:
        query_job = client.query(sql, job_config=job_config)
        rows = list(query_job.result())
        return rows
    except Exception as e:
        print("=== BIGQUERY ERROR START ===")
        print(str(e))
        print("=== BIGQUERY ERROR END ===")
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {str(e)}")


def normalize_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


# =============================
# Utils
# =============================

def build_date_ranges(start_date: Optional[str], end_date: Optional[str], days: int = 30):
    if start_date and end_date:
        return [{"startDate": start_date, "endDate": end_date}]
    return [{"startDate": f"{days}daysAgo", "endDate": "today"}]


def get_display_dimension(display_dimension: str = "pageTitle"):
    if display_dimension == "pagePath":
        return "pagePath"
    return "pageTitle"


def get_match_field(match_type: str = "url"):
    if match_type == "title":
        return "pageTitle"
    if match_type == "path":
        return "pagePath"
    return "pageLocation"


def build_string_filter(field_name: str, value: str, match_type: str = "EXACT"):
    return {
        "filter": {
            "fieldName": field_name,
            "stringFilter": {
                "matchType": match_type,
                "value": value
            }
        }
    }


def build_limit(limit: int):
    return str(limit)


# =============================
# Common Request Models
# =============================

class ChannelReportRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    limit: int = 20


class PageFlowRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    displayDimension: str = "pageTitle"
    limit: int = 20


class PageFlowFromPageRequest(BaseModel):
    sourcePage: str
    matchType: str = "path"
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    displayDimension: str = "pageTitle"
    limit: int = 20


class PreviousPageRequest(BaseModel):
    targetPage: str
    matchType: str = "url"
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    displayDimension: str = "pageTitle"
    limit: int = 20


class ConversionPagesRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    eventName: str = "generate_lead"
    displayDimension: str = "pageTitle"
    limit: int = 50


class ConversionPathRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    eventName: str = "generate_lead"
    displayDimension: str = "pageTitle"
    limit: int = 50


class ConversionSummaryRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    eventName: str = "generate_lead"


class ThanksPageSummaryRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    thanksPage: str = "/contact/thanks/"


class ExitPagesRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    displayDimension: str = "pageTitle"
    limit: int = 20


# =============================
# BigQuery Request Models
# =============================

class UsersByPageRequest(BaseModel):
    targetPage: str
    startDate: str
    endDate: str
    limit: int = 20
    matchType: str = "contains"  # contains / exact


class UserPathRequest(BaseModel):
    targetPage: str
    startDate: str
    endDate: str
    limitUsers: int = 20
    stepsPerUser: int = 10
    matchType: str = "contains"  # contains / exact


class UserJourneyRequest(BaseModel):
    userPseudoId: str
    startDate: str
    endDate: str
    limit: int = 50


# =============================
# Health
# =============================

@app.get("/health")
def health():
    return {
        "status": "ok"
    }


# =============================
# Channel Report
# =============================

@app.post("/api/ga4/standard/channel")
def channel_report(req: ChannelReportRequest):
    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "sessionDefaultChannelGroup"}
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"}
        ],
        "orderBys": [
            {
                "metric": {"metricName": "sessions"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# Page Flow (All)
# =============================

@app.post("/api/ga4/page/flow")
def page_flow(req: PageFlowRequest):
    display_dimension = get_display_dimension(req.displayDimension)

    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "pageReferrer"},
            {"name": display_dimension}
        ],
        "metrics": [
            {"name": "screenPageViews"}
        ],
        "orderBys": [
            {
                "metric": {"metricName": "screenPageViews"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# Page Flow From Specific Page
# =============================

@app.post("/api/ga4/page/flow/from-page")
def page_flow_from_page(req: PageFlowFromPageRequest):
    display_dimension = get_display_dimension(req.displayDimension)

    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "pageReferrer"},
            {"name": display_dimension}
        ],
        "metrics": [
            {"name": "screenPageViews"}
        ],
        "dimensionFilter": build_string_filter(
            field_name="pageReferrer",
            value=req.sourcePage,
            match_type="CONTAINS"
        ),
        "orderBys": [
            {
                "metric": {"metricName": "screenPageViews"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# Previous Pages Before Target Page
# =============================

@app.post("/api/ga4/page/before-page")
def previous_page(req: PreviousPageRequest):
    match_field = get_match_field(req.matchType)
    display_dimension = get_display_dimension(req.displayDimension)

    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "pageReferrer"},
            {"name": match_field},
            {"name": display_dimension}
        ],
        "metrics": [
            {"name": "screenPageViews"}
        ],
        "dimensionFilter": build_string_filter(
            field_name=match_field,
            value=req.targetPage,
            match_type="CONTAINS"
        ),
        "orderBys": [
            {
                "metric": {"metricName": "screenPageViews"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# Conversion Pages
# =============================

@app.post("/api/ga4/conversion/pages")
def conversion_pages(req: ConversionPagesRequest):
    display_dimension = get_display_dimension(req.displayDimension)

    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "eventName"},
            {"name": display_dimension}
        ],
        "metrics": [
            {"name": "eventCount"}
        ],
        "dimensionFilter": build_string_filter(
            field_name="eventName",
            value=req.eventName,
            match_type="EXACT"
        ),
        "orderBys": [
            {
                "metric": {"metricName": "eventCount"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# Conversion Path
# =============================

@app.post("/api/ga4/conversion/path")
def conversion_path(req: ConversionPathRequest):
    display_dimension = get_display_dimension(req.displayDimension)

    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "landingPage"},
            {"name": display_dimension}
        ],
        "metrics": [
            {"name": "eventCount"}
        ],
        "dimensionFilter": build_string_filter(
            field_name="eventName",
            value=req.eventName,
            match_type="EXACT"
        ),
        "orderBys": [
            {
                "metric": {"metricName": "eventCount"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# Conversion Summary
# =============================

@app.post("/api/ga4/conversion/summary")
def conversion_summary(req: ConversionSummaryRequest):
    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "eventName"}
        ],
        "metrics": [
            {"name": "eventCount"}
        ],
        "dimensionFilter": build_string_filter(
            field_name="eventName",
            value=req.eventName,
            match_type="EXACT"
        )
    }

    return call_ga4(body)


# =============================
# Thanks Page Summary
# =============================

@app.post("/api/ga4/conversion/thanks-summary")
def thanks_summary(req: ThanksPageSummaryRequest):
    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "screenPageViews"},
            {"name": "sessions"}
        ],
        "dimensionFilter": build_string_filter(
            field_name="pagePath",
            value=req.thanksPage,
            match_type="EXACT"
        )
    }

    return call_ga4(body)


# =============================
# Exit Pages
# =============================

@app.post("/api/ga4/page/exits")
def page_exits(req: ExitPagesRequest):
    display_dimension = get_display_dimension(req.displayDimension)

    body = {
        "dateRanges": build_date_ranges(req.startDate, req.endDate, req.days),
        "dimensions": [
            {"name": display_dimension}
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "screenPageViews"},
            {"name": "bounceRate"}
        ],
        "orderBys": [
            {
                "metric": {"metricName": "bounceRate"},
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =============================
# BigQuery: Users by Page
# =============================

@app.post("/api/bq/page/users")
def bq_users_by_page(req: UsersByPageRequest):
    table_suffix_start = normalize_yyyymmdd(req.startDate)
    table_suffix_end = normalize_yyyymmdd(req.endDate)

    sql = f"""
    SELECT
      user_pseudo_id,
      COUNT(*) AS page_views,
      MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_seen,
      MAX(TIMESTAMP_MICROS(event_timestamp)) AS last_seen
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN @startDate AND @endDate
      AND event_name = 'page_view'
      AND (
        SELECT ep.value.string_value
        FROM UNNEST(event_params) ep
        WHERE ep.key = 'page_location'
      ) LIKE @targetPageLike
    GROUP BY user_pseudo_id
    ORDER BY page_views DESC, last_seen DESC
    LIMIT @limit
    """

    params = [
        bigquery.ScalarQueryParameter("startDate", "STRING", table_suffix_start),
        bigquery.ScalarQueryParameter("endDate", "STRING", table_suffix_end),
        bigquery.ScalarQueryParameter("limit", "INT64", req.limit),
        bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%")
    ]

    rows = run_bq_query(sql, params)

    return {
        "rows": [
            {
                "userPseudoId": row["user_pseudo_id"],
                "pageViews": row["page_views"],
                "firstSeen": row["first_seen"].isoformat() if row["first_seen"] else None,
                "lastSeen": row["last_seen"].isoformat() if row["last_seen"] else None
            }
            for row in rows
        ]
    }


# =============================
# BigQuery: User Paths by Target Page
# =============================

@app.post("/api/bq/user/path")
def bq_user_path(req: UserPathRequest):
    table_suffix_start = normalize_yyyymmdd(req.startDate)
    table_suffix_end = normalize_yyyymmdd(req.endDate)

    sql = f"""
    WITH target_users AS (
      SELECT DISTINCT user_pseudo_id
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN @startDate AND @endDate
        AND event_name = 'page_view'
        AND (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
      LIMIT @limitUsers
    ),
    page_events AS (
      SELECT
        e.user_pseudo_id,
        TIMESTAMP_MICROS(e.event_timestamp) AS event_time,
        (
          SELECT ep.value.int_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'ga_session_id'
        ) AS ga_session_id,
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) AS page_location,
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_title'
        ) AS page_title
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*` e
      INNER JOIN target_users tu
        ON e.user_pseudo_id = tu.user_pseudo_id
      WHERE
        _TABLE_SUFFIX BETWEEN @startDate AND @endDate
        AND e.event_name = 'page_view'
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY user_pseudo_id
          ORDER BY event_time DESC
        ) AS rn
      FROM page_events
    )
    SELECT
      user_pseudo_id,
      ga_session_id,
      event_time,
      page_location,
      page_title
    FROM ranked
    WHERE rn <= @stepsPerUser
    ORDER BY user_pseudo_id, event_time ASC
    """

    params = [
        bigquery.ScalarQueryParameter("startDate", "STRING", table_suffix_start),
        bigquery.ScalarQueryParameter("endDate", "STRING", table_suffix_end),
        bigquery.ScalarQueryParameter("limitUsers", "INT64", req.limitUsers),
        bigquery.ScalarQueryParameter("stepsPerUser", "INT64", req.stepsPerUser),
        bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%")
    ]

    rows = run_bq_query(sql, params)

    grouped = {}
    for row in rows:
        user_id = row["user_pseudo_id"]
        if user_id not in grouped:
            grouped[user_id] = []

        grouped[user_id].append({
            "sessionId": row["ga_session_id"],
            "eventTime": row["event_time"].isoformat() if row["event_time"] else None,
            "pageLocation": row["page_location"],
            "pageTitle": row["page_title"]
        })

    return {
        "rows": [
            {
                "userPseudoId": user_id,
                "journey": journey
            }
            for user_id, journey in grouped.items()
        ]
    }


# =============================
# BigQuery: Single User Journey
# =============================

@app.post("/api/bq/user/journey")
def bq_single_user_journey(req: UserJourneyRequest):
    table_suffix_start = normalize_yyyymmdd(req.startDate)
    table_suffix_end = normalize_yyyymmdd(req.endDate)

    sql = f"""
    SELECT
      user_pseudo_id,
      TIMESTAMP_MICROS(event_timestamp) AS event_time,
      (
        SELECT ep.value.int_value
        FROM UNNEST(event_params) ep
        WHERE ep.key = 'ga_session_id'
      ) AS ga_session_id,
      (
        SELECT ep.value.string_value
        FROM UNNEST(event_params) ep
        WHERE ep.key = 'page_location'
      ) AS page_location,
      (
        SELECT ep.value.string_value
        FROM UNNEST(event_params) ep
        WHERE ep.key = 'page_title'
      ) AS page_title
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN @startDate AND @endDate
      AND event_name = 'page_view'
      AND user_pseudo_id = @userPseudoId
    ORDER BY event_time ASC
    LIMIT @limit
    """

    params = [
        bigquery.ScalarQueryParameter("startDate", "STRING", table_suffix_start),
        bigquery.ScalarQueryParameter("endDate", "STRING", table_suffix_end),
        bigquery.ScalarQueryParameter("userPseudoId", "STRING", req.userPseudoId),
        bigquery.ScalarQueryParameter("limit", "INT64", req.limit),
    ]

    rows = run_bq_query(sql, params)

    return {
        "rows": [
            {
                "userPseudoId": row["user_pseudo_id"],
                "sessionId": row["ga_session_id"],
                "eventTime": row["event_time"].isoformat() if row["event_time"] else None,
                "pageLocation": row["page_location"],
                "pageTitle": row["page_title"]
            }
            for row in rows
        ]
    }
