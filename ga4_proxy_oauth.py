from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
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


def build_bq_date_condition(
    start_date: Optional[str],
    end_date: Optional[str],
    field_name: str = "_TABLE_SUFFIX"
):
    conditions = []
    params = []

    if start_date:
        conditions.append(f"{field_name} >= @startDate")
        params.append(
            bigquery.ScalarQueryParameter("startDate", "STRING", normalize_yyyymmdd(start_date))
        )

    if end_date:
        conditions.append(f"{field_name} <= @endDate")
        params.append(
            bigquery.ScalarQueryParameter("endDate", "STRING", normalize_yyyymmdd(end_date))
        )

    if not conditions:
        return "1=1", params

    return " AND ".join(conditions), params


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


def get_ga4_match_type(match_type: str = "contains"):
    match_type = (match_type or "contains").lower()
    if match_type == "exact":
        return "EXACT"
    return "CONTAINS"


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
    days: int = Field(default=30, ge=1, le=365)
    limit: int = Field(default=20, ge=1, le=100)


class PageFlowRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: str = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class PageFlowFromPageRequest(BaseModel):
    sourcePage: str
    matchType: str = "contains"  # contains / exact
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: str = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class PreviousPageRequest(BaseModel):
    targetPage: str
    matchType: str = "url"  # url / path / title
    filterMatchType: str = "contains"  # contains / exact
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: str = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class ConversionPagesRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    eventName: str = "generate_lead"
    displayDimension: str = "pageTitle"
    limit: int = Field(default=50, ge=1, le=100)


class ConversionPathRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    eventName: str = "generate_lead"
    displayDimension: str = "pageTitle"
    limit: int = Field(default=50, ge=1, le=100)


class ConversionSummaryRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    eventName: str = "generate_lead"


class ThanksPageSummaryRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    thanksPage: str = "/contact/thanks/"


class ExitPagesRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: str = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


# =============================
# BigQuery Request Models
# =============================

class UsersByPageRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limit: int = Field(default=20, ge=1, le=100)
    matchType: str = "contains"  # contains / exact


class UserPathRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limitUsers: int = Field(default=20, ge=1, le=100)
    stepsPerUser: int = Field(default=10, ge=1, le=20)
    matchType: str = "contains"  # contains / exact


class UserJourneyRequest(BaseModel):
    userPseudoId: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limit: int = Field(default=50, ge=1, le=200)


class PrePagesBeforeTargetRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limitUsers: int = Field(default=20, ge=1, le=100)
    stepsPerUser: int = Field(default=5, ge=1, le=10)
    matchType: str = "contains"  # contains / exact


class ConversionPrePagesRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limitUsers: int = Field(default=50, ge=1, le=100)
    stepsPerUser: int = Field(default=5, ge=1, le=10)
    matchType: str = "contains"
    excludePages: Optional[list[str]] = []


# =============================
# Health
# =============================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "bq-all-period-20260310-3"
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
    ga4_match_type = get_ga4_match_type(req.matchType)

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
            match_type=ga4_match_type
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
    ga4_match_type = get_ga4_match_type(req.filterMatchType)

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
            match_type=ga4_match_type
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
    date_condition, date_params = build_bq_date_condition(req.startDate, req.endDate)

    if req.matchType == "exact":
        page_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """
        page_params = [
            bigquery.ScalarQueryParameter("targetPage", "STRING", req.targetPage)
        ]
    else:
        page_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
        """
       page_params = [
    bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%")
]

exclude_condition = ""

if req.excludePages:
    conditions = [f"page_location LIKE '%{p}%'" for p in req.excludePages]
    exclude_condition = "AND NOT (" + " OR ".join(conditions) + ")"

sql = f"""
SELECT
  page_location,
  page_title,
  COUNT(*) AS appearance_count,
  COUNT(DISTINCT user_pseudo_id) AS users_count
FROM ranked
WHERE rn_desc <= @stepsPerUser
{exclude_condition}
GROUP BY page_location, page_title
ORDER BY users_count DESC, appearance_count DESC
LIMIT 100
"""

    params = date_params + page_params + [
        bigquery.ScalarQueryParameter("limit", "INT64", req.limit)
    ]

    rows = run_bq_query(sql, params)

    return {
        "count": len(rows),
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
    date_condition, date_params = build_bq_date_condition(req.startDate, req.endDate)

    if req.matchType == "exact":
        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """
        target_params = [
            bigquery.ScalarQueryParameter("targetPage", "STRING", req.targetPage)
        ]
    else:
        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
        """
        target_params = [
            bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%")
        ]

    sql = f"""
    WITH target_users AS (
      SELECT DISTINCT user_pseudo_id
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
      WHERE
        {date_condition}
        AND event_name = 'page_view'
        AND {target_condition}
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
        {date_condition.replace("_TABLE_SUFFIX", "e._TABLE_SUFFIX")}
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

    params = date_params + target_params + [
        bigquery.ScalarQueryParameter("limitUsers", "INT64", req.limitUsers),
        bigquery.ScalarQueryParameter("stepsPerUser", "INT64", req.stepsPerUser)
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
        "count": len(grouped),
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
    date_condition, date_params = build_bq_date_condition(req.startDate, req.endDate)

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
      {date_condition}
      AND event_name = 'page_view'
      AND user_pseudo_id = @userPseudoId
    ORDER BY event_time ASC
    LIMIT @limit
    """

    params = date_params + [
        bigquery.ScalarQueryParameter("userPseudoId", "STRING", req.userPseudoId),
        bigquery.ScalarQueryParameter("limit", "INT64", req.limit),
    ]

    rows = run_bq_query(sql, params)

    return {
        "count": len(rows),
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


# =============================
# BigQuery: Pre Pages Before Target
# =============================

@app.post("/api/bq/page/pre-pages")
def bq_pre_pages_before_target(req: PrePagesBeforeTargetRequest):
    date_condition, date_params = build_bq_date_condition(req.startDate, req.endDate)

    if req.matchType == "exact":
        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """
        target_params = [
            bigquery.ScalarQueryParameter("targetPage", "STRING", req.targetPage)
        ]
    else:
        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
        """
        target_params = [
            bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%")
        ]

    sql = f"""
    WITH target_hits AS (
      SELECT
        user_pseudo_id,
        TIMESTAMP_MICROS(event_timestamp) AS target_time
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
      WHERE
        {date_condition}
        AND event_name = 'page_view'
        AND {target_condition}
    ),
    target_users AS (
      SELECT
        user_pseudo_id,
        MAX(target_time) AS latest_target_time
      FROM target_hits
      GROUP BY user_pseudo_id
      ORDER BY latest_target_time DESC
      LIMIT @limitUsers
    ),
    page_events AS (
      SELECT
        e.user_pseudo_id,
        TIMESTAMP_MICROS(e.event_timestamp) AS event_time,
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
        {date_condition.replace("_TABLE_SUFFIX", "e._TABLE_SUFFIX")}
        AND e.event_name = 'page_view'
        AND TIMESTAMP_MICROS(e.event_timestamp) <= tu.latest_target_time
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY user_pseudo_id
          ORDER BY event_time DESC
        ) AS rn_desc
      FROM page_events
    )
    SELECT
      user_pseudo_id,
      event_time,
      page_location,
      page_title
    FROM ranked
    WHERE rn_desc <= @stepsPerUser
    ORDER BY user_pseudo_id, event_time ASC
    """

    params = date_params + target_params + [
        bigquery.ScalarQueryParameter("limitUsers", "INT64", req.limitUsers),
        bigquery.ScalarQueryParameter("stepsPerUser", "INT64", req.stepsPerUser)
    ]

    rows = run_bq_query(sql, params)

    grouped = {}
    for row in rows:
        user_id = row["user_pseudo_id"]
        if user_id not in grouped:
            grouped[user_id] = []

        grouped[user_id].append({
            "eventTime": row["event_time"].isoformat() if row["event_time"] else None,
            "pageLocation": row["page_location"],
            "pageTitle": row["page_title"]
        })

    return {
        "count": len(grouped),
        "rows": [
            {
                "userPseudoId": user_id,
                "prePages": pages
            }
            for user_id, pages in grouped.items()
        ]
    }


# =============================
# BigQuery: Conversion Pre Pages
# =============================

@app.post("/api/bq/conversion/pre-pages")
def bq_conversion_pre_pages(req: ConversionPrePagesRequest):
    date_condition, date_params = build_bq_date_condition(req.startDate, req.endDate)

    if req.matchType == "exact":
        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """
        target_params = [
            bigquery.ScalarQueryParameter("targetPage", "STRING", req.targetPage)
        ]
        exclude_target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) != @targetPage
        """
    else:
        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
        """
        target_params = [
            bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%")
        ]
        exclude_target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) NOT LIKE @targetPageLike
        """

    sql = f"""
    WITH target_hits AS (
      SELECT
        user_pseudo_id,
        TIMESTAMP_MICROS(event_timestamp) AS target_time
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
      WHERE
        {date_condition}
        AND event_name = 'page_view'
        AND {target_condition}
    ),
    latest_target_per_user AS (
      SELECT
        user_pseudo_id,
        MAX(target_time) AS latest_target_time
      FROM target_hits
      GROUP BY user_pseudo_id
      ORDER BY latest_target_time DESC
      LIMIT @limitUsers
    ),
    page_events AS (
      SELECT
        e.user_pseudo_id,
        TIMESTAMP_MICROS(e.event_timestamp) AS event_time,
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
      INNER JOIN latest_target_per_user t
        ON e.user_pseudo_id = t.user_pseudo_id
      WHERE
        {date_condition.replace("_TABLE_SUFFIX", "e._TABLE_SUFFIX")}
        AND e.event_name = 'page_view'
        AND TIMESTAMP_MICROS(e.event_timestamp) < t.latest_target_time
        AND {exclude_target_condition}
　　　　　　　{exclude_condition}
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY user_pseudo_id
          ORDER BY event_time DESC
        ) AS rn_desc
      FROM page_events
    )
    SELECT
      page_location,
      page_title,
      COUNT(*) AS appearance_count,
      COUNT(DISTINCT user_pseudo_id) AS users_count
    FROM ranked
    WHERE rn_desc <= @stepsPerUser
    GROUP BY page_location, page_title
    ORDER BY users_count DESC, appearance_count DESC
    LIMIT 100
    """

    params = date_params + target_params + [
        bigquery.ScalarQueryParameter("limitUsers", "INT64", req.limitUsers),
        bigquery.ScalarQueryParameter("stepsPerUser", "INT64", req.stepsPerUser)
    ]

    rows = run_bq_query(sql, params)

    return {
        "count": len(rows),
        "rows": [
            {
                "pageLocation": row["page_location"],
                "pageTitle": row["page_title"],
                "appearanceCount": row["appearance_count"],
                "usersCount": row["users_count"]
            }
            for row in rows
        ]
    }
