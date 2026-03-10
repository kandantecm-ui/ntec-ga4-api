from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
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

    r = requests.post(url, data=payload)

    if r.status_code != 200:
        raise Exception(f"Token refresh failed: {r.text}")

    return r.json()["access_token"]


def get_access_token():
    if GOOGLE_ACCESS_TOKEN:
        return GOOGLE_ACCESS_TOKEN

    return refresh_access_token()


def call_ga4(data):
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

    access_token = get_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{GA4_PROPERTY_ID}:runReport"

    r = requests.post(url, headers=headers, json=data)

    if r.status_code == 401:
        access_token = refresh_access_token()
        headers["Authorization"] = f"Bearer {access_token}"
        r = requests.post(url, headers=headers, json=data)

    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    return r.json()


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


def run_bq_query(sql, params):
    client = get_bq_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=params
    )

    try:
        query_job = client.query(sql, job_config=job_config)
        return list(query_job.result())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {str(e)}")


def normalize_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/ga4/standard/channel")
def channel_report():
    body = {
        "dateRanges": [
            {"startDate": "30daysAgo", "endDate": "today"}
        ],
        "dimensions": [
            {"name": "sessionDefaultChannelGroup"}
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"}
        ]
    }

    return call_ga4(body)


@app.post("/api/ga4/page/flow")
def page_flow():
    body = {
        "dateRanges": [
            {"startDate": "30daysAgo", "endDate": "today"}
        ],
        "dimensions": [
            {"name": "pageReferrer"},
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "screenPageViews"}
        ]
    }

    return call_ga4(body)


@app.post("/api/ga4/conversion/pages")
def conversion_pages():
    body = {
        "dateRanges": [
            {"startDate": "30daysAgo", "endDate": "today"}
        ],
        "dimensions": [
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "sessions"}
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "eventName",
                "stringFilter": {
                    "matchType": "EXACT",
                    "value": "generate_lead"
                }
            }
        }
    }

    return call_ga4(body)


@app.post("/api/ga4/conversion/path")
def conversion_path():
    body = {
        "dateRanges": [
            {"startDate": "30daysAgo", "endDate": "today"}
        ],
        "dimensions": [
            {"name": "landingPage"},
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "sessions"}
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "eventName",
                "stringFilter": {
                    "matchType": "EXACT",
                    "value": "generate_lead"
                }
            }
        }
    }

    return call_ga4(body)


class ConversionSummaryRequest(BaseModel):
    days: int = 30
    eventName: str = "generate_lead"


@app.post("/api/ga4/conversion/summary")
def conversion_summary(req: ConversionSummaryRequest):
    body = {
        "dateRanges": [
            {
                "startDate": f"{req.days}daysAgo",
                "endDate": "today"
            }
        ],
        "dimensions": [
            {"name": "eventName"}
        ],
        "metrics": [
            {"name": "eventCount"}
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "eventName",
                "stringFilter": {
                    "matchType": "EXACT",
                    "value": req.eventName
                }
            }
        }
    }

    return call_ga4(body)


class ThanksPageSummaryRequest(BaseModel):
    days: int = 30
    thanksPage: str = "/contact/thanks/"


@app.post("/api/ga4/conversion/thanks-summary")
def thanks_summary(req: ThanksPageSummaryRequest):
    body = {
        "dateRanges": [
            {
                "startDate": f"{req.days}daysAgo",
                "endDate": "today"
            }
        ],
        "dimensions": [
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "screenPageViews"},
            {"name": "sessions"}
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "pagePath",
                "stringFilter": {
                    "matchType": "EXACT",
                    "value": req.thanksPage
                }
            }
        }
    }

    return call_ga4(body)


class PageFlowFromPageRequest(BaseModel):
    sourcePage: str
    days: int = 30
    limit: int = 20


@app.post("/api/ga4/page/flow/from-page")
def page_flow_from_page(req: PageFlowFromPageRequest):
    body = {
        "dateRanges": [
            {
                "startDate": f"{req.days}daysAgo",
                "endDate": "today"
            }
        ],
        "dimensions": [
            {"name": "pageReferrer"},
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "screenPageViews"}
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "pageReferrer",
                "stringFilter": {
                    "matchType": "CONTAINS",
                    "value": req.sourcePage
                }
            }
        },
        "orderBys": [
            {
                "metric": {
                    "metricName": "screenPageViews"
                },
                "desc": True
            }
        ],
        "limit": str(req.limit)
    }

    return call_ga4(body)


class ExitPagesRequest(BaseModel):
    days: int = 30
    limit: int = 20


@app.post("/api/ga4/page/exits")
def page_exits(req: ExitPagesRequest):
    body = {
        "dateRanges": [
            {
                "startDate": f"{req.days}daysAgo",
                "endDate": "today"
            }
        ],
        "dimensions": [
            {"name": "pagePath"}
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "screenPageViews"},
            {"name": "bounceRate"}
        ],
        "metricFilter": {
            "filter": {
                "fieldName": "sessions",
                "numericFilter": {
                    "operation": "GREATER_THAN",
                    "value": {
                        "int64Value": "10"
                    }
                }
            }
        },
        "orderBys": [
            {
                "metric": {
                    "metricName": "bounceRate"
                },
                "desc": True
            }
        ],
        "limit": str(req.limit)
    }

    return call_ga4(body)


# -----------------------------
# BigQuery: User Tracking
# -----------------------------

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


@app.post("/api/bq/page/users")
def bq_users_by_page(req: UsersByPageRequest):
    table_suffix_start = normalize_yyyymmdd(req.startDate)
    table_suffix_end = normalize_yyyymmdd(req.endDate)

    where_clause = "page_location = @targetPage" if req.matchType == "exact" else "page_location LIKE @targetPageLike"

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
      AND {where_clause}
    GROUP BY user_pseudo_id
    ORDER BY page_views DESC, last_seen DESC
    LIMIT @limit
    """

    params = [
        bigquery.ScalarQueryParameter("startDate", "STRING", table_suffix_start),
        bigquery.ScalarQueryParameter("endDate", "STRING", table_suffix_end),
        bigquery.ScalarQueryParameter("limit", "INT64", req.limit),
    ]

    if req.matchType == "exact":
        params.append(bigquery.ScalarQueryParameter("targetPage", "STRING", req.targetPage))
    else:
        params.append(bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%"))

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


@app.post("/api/bq/user/path")
def bq_user_path(req: UserPathRequest):
    table_suffix_start = normalize_yyyymmdd(req.startDate)
    table_suffix_end = normalize_yyyymmdd(req.endDate)

    where_clause = "page_location = @targetPage" if req.matchType == "exact" else "page_location LIKE @targetPageLike"

    sql = f"""
    WITH target_users AS (
      SELECT DISTINCT user_pseudo_id
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN @startDate AND @endDate
        AND event_name = 'page_view'
        AND {where_clause}
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
    ]

    if req.matchType == "exact":
        params.append(bigquery.ScalarQueryParameter("targetPage", "STRING", req.targetPage))
    else:
        params.append(bigquery.ScalarQueryParameter("targetPageLike", "STRING", f"%{req.targetPage}%"))

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
