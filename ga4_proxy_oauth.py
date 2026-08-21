from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Literal

from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient

import requests
import os
import json


app = FastAPI(
    title="NTEC Analytics API",
    version="2026.08.19"
)


# =========================================================
# Environment Variables
# =========================================================

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")

GOOGLE_ACCESS_TOKEN = os.getenv("GOOGLE_ACCESS_TOKEN")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")

# =========================================================
# OAuth
# =========================================================

def refresh_access_token():
    if not GOOGLE_REFRESH_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_REFRESH_TOKEN not set"
        )

    if not GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_CLIENT_ID not set"
        )

    if not GOOGLE_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_CLIENT_SECRET not set"
        )

    url = "https://oauth2.googleapis.com/token"

    payload = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }

    try:
        response = requests.post(
            url,
            data=payload,
            timeout=60
        )
    except requests.RequestException as e:
        print("=== GOOGLE TOKEN REQUEST ERROR ===")
        print(str(e))

        raise HTTPException(
            status_code=502,
            detail=f"Google OAuth request failed: {str(e)}"
        )

    if response.status_code != 200:
        print("=== GOOGLE TOKEN REFRESH ERROR ===")
        print("status:", response.status_code)
        print("body:", response.text)

        raise HTTPException(
            status_code=500,
            detail=f"Token refresh failed: {response.text}"
        )

    try:
        data = response.json()
        access_token = data["access_token"]
        return access_token

    except Exception as e:
        print("=== GOOGLE TOKEN PARSE ERROR ===")
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=f"Access token parse failed: {str(e)}"
        )


def get_access_token():
    # Refresh Token方式を優先
    if (
        GOOGLE_REFRESH_TOKEN
        and GOOGLE_CLIENT_ID
        and GOOGLE_CLIENT_SECRET
    ):
        return refresh_access_token()

    # 一時的なAccess Tokenが設定されている場合
    if GOOGLE_ACCESS_TOKEN:
        return GOOGLE_ACCESS_TOKEN

    raise HTTPException(
        status_code=500,
        detail=(
            "Google OAuth credentials not set. "
            "Set GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, "
            "GOOGLE_CLIENT_SECRET or GOOGLE_ACCESS_TOKEN."
        )
    )


# =========================================================
# GA4 Core
# =========================================================

def call_ga4(data: dict):

    if not GA4_PROPERTY_ID:
        raise HTTPException(
            status_code=500,
            detail="GA4_PROPERTY_ID not set"
        )

    try:
        access_token = get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        url = (
            "https://analyticsdata.googleapis.com/v1beta/"
            f"properties/{GA4_PROPERTY_ID}:runReport"
        )

        response = requests.post(
            url,
            headers=headers,
            json=data,
            timeout=120
        )

        # Access Token期限切れ等の場合、Refresh Tokenがあれば再試行
        if (
            response.status_code == 401
            and GOOGLE_REFRESH_TOKEN
            and GOOGLE_CLIENT_ID
            and GOOGLE_CLIENT_SECRET
        ):
            access_token = refresh_access_token()

            headers["Authorization"] = (
                f"Bearer {access_token}"
            )

            response = requests.post(
                url,
                headers=headers,
                json=data,
                timeout=120
            )

        if response.status_code != 200:

            print("=== GA4 API ERROR START ===")
            print("status:", response.status_code)
            print("property:", GA4_PROPERTY_ID)
            print("request:", json.dumps(data, ensure_ascii=False))
            print("response:", response.text)
            print("=== GA4 API ERROR END ===")

            raise HTTPException(
                status_code=response.status_code,
                detail={
                    "error": "GA4_API_ERROR",
                    "status": response.status_code,
                    "message": response.text
                }
            )

        return response.json()

    except HTTPException:
        raise

    except requests.RequestException as e:

        print("=== GA4 NETWORK ERROR ===")
        print(str(e))

        raise HTTPException(
            status_code=502,
            detail=f"GA4 request failed: {str(e)}"
        )

    except Exception as e:

        print("=== GA4 UNEXPECTED ERROR ===")
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=f"GA4 unexpected error: {str(e)}"
        )


# =========================================================
# BigQuery Core
# =========================================================

def get_bq_client():

    if not BIGQUERY_PROJECT_ID:
        raise HTTPException(
            status_code=500,
            detail="BIGQUERY_PROJECT_ID not set"
        )

    if not BIGQUERY_DATASET:
        raise HTTPException(
            status_code=500,
            detail="BIGQUERY_DATASET not set"
        )

    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_SERVICE_ACCOUNT_JSON not set"
        )

    try:
        info = json.loads(
            GOOGLE_SERVICE_ACCOUNT_JSON
        )

        credentials = (
            service_account
            .Credentials
            .from_service_account_info(info)
        )

        client = bigquery.Client(
            project=BIGQUERY_PROJECT_ID,
            credentials=credentials
        )

        return client

    except Exception as e:

        print("=== BIGQUERY CLIENT ERROR ===")
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "BigQuery client init failed: "
                f"{str(e)}"
            )
        )


def run_bq_query(sql: str, params: list):

    client = get_bq_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=params
    )

    try:
        query_job = client.query(
            sql,
            job_config=job_config
        )

        rows = list(
            query_job.result()
        )

        return rows

    except Exception as e:

        print("=== BIGQUERY QUERY ERROR START ===")
        print(type(e).__name__)
        print(str(e))
        print("=== BIGQUERY QUERY ERROR END ===")

        raise HTTPException(
            status_code=500,
            detail=f"BigQuery query failed: {str(e)}"
        )


# =========================================================
# Search Console Core
# =========================================================

def get_search_console_service():

    try:
        access_token = get_access_token()

        credentials = Credentials(
            token=access_token
        )

        service = build(
            "searchconsole",
            "v1",
            credentials=credentials,
            cache_discovery=False
        )

        return service

    except HTTPException:
        raise

    except Exception as e:

        print("=== SEARCH CONSOLE INIT ERROR ===")
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "Search Console service init failed: "
                f"{str(e)}"
            )
        )

# =========================================================
# Google Ads Core
# =========================================================

def get_google_ads_client():

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_ADS_DEVELOPER_TOKEN not set"
        )

    if not GOOGLE_ADS_CUSTOMER_ID:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_ADS_CUSTOMER_ID not set"
        )

    if not GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_CLIENT_ID not set"
        )

    if not GOOGLE_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_CLIENT_SECRET not set"
        )

    if not GOOGLE_REFRESH_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_REFRESH_TOKEN not set"
        )

    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
        "use_proto_plus": True
    }

    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = (
            GOOGLE_ADS_LOGIN_CUSTOMER_ID.replace("-", "")
        )

    try:
        client = GoogleAdsClient.load_from_dict(config)
        return client

    except Exception as e:
        print("=== GOOGLE ADS CLIENT ERROR ===")
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=f"Google Ads client init failed: {str(e)}"
        )

# =========================================================
# Utils
# =========================================================

def normalize_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


def validate_ga4_date_pair(
    start_date: Optional[str],
    end_date: Optional[str]
):
    if bool(start_date) != bool(end_date):
        raise HTTPException(
            status_code=400,
            detail=(
                "startDate and endDate must be "
                "specified together"
            )
        )


def build_bq_date_condition(
    start_date: Optional[str],
    end_date: Optional[str],
    field_name: str = "_TABLE_SUFFIX"
):
    conditions = []
    params = []

    if start_date:
        conditions.append(
            f"{field_name} >= @startDate"
        )

        params.append(
            bigquery.ScalarQueryParameter(
                "startDate",
                "STRING",
                normalize_yyyymmdd(start_date)
            )
        )

    if end_date:
        conditions.append(
            f"{field_name} <= @endDate"
        )

        params.append(
            bigquery.ScalarQueryParameter(
                "endDate",
                "STRING",
                normalize_yyyymmdd(end_date)
            )
        )

    if not conditions:
        return "1=1", params

    return " AND ".join(conditions), params


def build_date_ranges(
    start_date: Optional[str],
    end_date: Optional[str],
    days: int = 30
):
    validate_ga4_date_pair(
        start_date,
        end_date
    )

    if start_date and end_date:
        return [
            {
                "startDate": start_date,
                "endDate": end_date
            }
        ]

    return [
        {
            "startDate": f"{days}daysAgo",
            "endDate": "today"
        }
    ]


def get_display_dimension(
    display_dimension: str = "pageTitle"
):
    if display_dimension == "pagePath":
        return "pagePath"

    return "pageTitle"


def get_match_field(
    match_type: str = "url"
):
    if match_type == "title":
        return "pageTitle"

    if match_type == "path":
        return "pagePath"

    return "pageLocation"


def get_ga4_match_type(
    match_type: str = "contains"
):
    if (
        match_type or "contains"
    ).lower() == "exact":
        return "EXACT"

    return "CONTAINS"


def build_string_filter(
    field_name: str,
    value: str,
    match_type: str = "EXACT"
):
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


def build_bq_exclude_conditions(
    exclude_pages: list[str],
    alias: str = "e"
):
    if not exclude_pages:
        return "", []

    conditions = []
    params = []

    for index, page in enumerate(
        exclude_pages
    ):
        param_name = (
            f"excludePage{index}"
        )

        conditions.append(
            f"""
            (
              SELECT ep.value.string_value
              FROM UNNEST({alias}.event_params) ep
              WHERE ep.key = 'page_location'
            ) NOT LIKE @{param_name}
            """
        )

        params.append(
            bigquery.ScalarQueryParameter(
                param_name,
                "STRING",
                f"%{page}%"
            )
        )

    return (
        " AND " + " AND ".join(conditions),
        params
    )

# =========================================================
# Dashboard Utils
# =========================================================

def calculate_previous_period(
    start_date: Optional[str],
    end_date: Optional[str],
    days: int
):
    if start_date and end_date:
        start = datetime.strptime(
            start_date,
            "%Y-%m-%d"
        )

        end = datetime.strptime(
            end_date,
            "%Y-%m-%d"
        )

        period_days = (
            end - start
        ).days + 1

        previous_end = (
            start - timedelta(days=1)
        )

        previous_start = (
            previous_end
            - timedelta(days=period_days - 1)
        )

        return {
            "startDate":
                previous_start.strftime(
                    "%Y-%m-%d"
                ),

            "endDate":
                previous_end.strftime(
                    "%Y-%m-%d"
                )
        }

    return {
        "startDate":
            f"{days * 2}daysAgo",

        "endDate":
            f"{days + 1}daysAgo"
    }


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def percent_change(
    current,
    previous
):
    current = safe_float(current)
    previous = safe_float(previous)

    if previous == 0:
        if current == 0:
            return 0.0
        return None

    return round(
        (
            (current - previous)
            / previous
        ) * 100,
        1
    )


def health_from_change(
    change_percent,
    positive_is_good=True
):
    if change_percent is None:
        return "unknown"

    change = (
        change_percent
        if positive_is_good
        else -change_percent
    )

    if change >= 5:
        return "good"

    if change <= -5:
        return "warning"

    return "neutral"


def get_metric_value(
    response: dict,
    metric_index: int
):
    rows = response.get(
        "rows",
        []
    )

    if not rows:
        return 0

    metric_values = rows[0].get(
        "metricValues",
        []
    )

    if len(metric_values) <= metric_index:
        return 0

    return metric_values[
        metric_index
    ].get(
        "value",
        0
    )


def extract_channel_rows(
    response: dict
):
    result = []

    for row in response.get(
        "rows",
        []
    ):
        dimensions = row.get(
            "dimensionValues",
            []
        )

        metrics = row.get(
            "metricValues",
            []
        )

        channel = (
            dimensions[0].get(
                "value",
                "(not set)"
            )
            if dimensions
            else "(not set)"
        )

        sessions = (
            safe_int(
                metrics[0].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 0
            else 0
        )

        users = (
            safe_int(
                metrics[1].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 1
            else 0
        )

        result.append(
            {
                "channel": channel,
                "sessions": sessions,
                "users": users
            }
        )

    return result


def build_business_questions(
    kpis: dict,
    channels: list
):
    questions = []

    sessions_change = (
        kpis
        .get("sessions", {})
        .get("changePercent")
    )

    users_change = (
        kpis
        .get("users", {})
        .get("changePercent")
    )

    if (
        sessions_change is not None
        and sessions_change < -5
    ):
        questions.append(
            {
                "id": "traffic-down",
                "question":
                    "なぜセッション数が前期間より減っている？",
                "drilldown":
                    "channel"
            }
        )

    if (
        users_change is not None
        and users_change < -5
    ):
        questions.append(
            {
                "id": "users-down",
                "question":
                    "新規・既存を含むユーザー数減少の主因はどのチャネル？",
                "drilldown":
                    "channel"
            }
        )

    unassigned = next(
        (
            row
            for row in channels
            if row["channel"]
            == "Unassigned"
        ),
        None
    )

    total_sessions = sum(
        row["sessions"]
        for row in channels
    )

    if (
        unassigned
        and total_sessions > 0
    ):
        ratio = (
            unassigned["sessions"]
            / total_sessions
        ) * 100

        if ratio >= 5:
            questions.append(
                {
                    "id":
                        "unassigned-high",

                    "question":
                        "Unassigned流入が多いのはなぜ？UTMや流入元分類に問題はない？",

                    "drilldown":
                        "unassigned"
                }
            )

    paid_social = next(
        (
            row
            for row in channels
            if row["channel"]
            == "Paid Social"
        ),
        None
    )

    if paid_social:
        questions.append(
            {
                "id":
                    "paid-social-conversion",

                "question":
                    "Paid Socialから来たユーザーはイベント閲覧や予約につながっている？",

                "drilldown":
                    "conversion"
            }
        )

    organic = next(
        (
            row
            for row in channels
            if row["channel"]
            == "Organic Search"
        ),
        None
    )

    if organic:
        questions.append(
            {
                "id":
                    "organic-conversion",

                "question":
                    "Organic Searchから予約につながっているページはどれ？",

                "drilldown":
                    "conversion"
            }
        )

    questions.append(
        {
            "id":
                "reservation-path",

            "question":
                "予約完了ユーザーは直前にどのページを見ている？",

            "drilldown":
                "conversion-pre-pages"
        }
    )

    return questions[:6]

def build_channel_comparison(
    current_channels: list,
    previous_channels: list
):
    current_map = {
        row["channel"]: row
        for row in current_channels
    }

    previous_map = {
        row["channel"]: row
        for row in previous_channels
    }

    all_channels = set(
        current_map.keys()
    ) | set(
        previous_map.keys()
    )

    result = []

    for channel in all_channels:
        current = current_map.get(
            channel,
            {
                "sessions": 0,
                "users": 0
            }
        )

        previous = previous_map.get(
            channel,
            {
                "sessions": 0,
                "users": 0
            }
        )

        current_sessions = current.get(
            "sessions",
            0
        )

        previous_sessions = previous.get(
            "sessions",
            0
        )

        current_users = current.get(
            "users",
            0
        )

        previous_users = previous.get(
            "users",
            0
        )

        sessions_change = percent_change(
            current_sessions,
            previous_sessions
        )

        users_change = percent_change(
            current_users,
            previous_users
        )

        result.append(
            {
                "channel": channel,

                "currentSessions":
                    current_sessions,

                "previousSessions":
                    previous_sessions,

                "sessionsChangePercent":
                    sessions_change,

                "currentUsers":
                    current_users,

                "previousUsers":
                    previous_users,

                "usersChangePercent":
                    users_change,

                "status":
                    health_from_change(
                        sessions_change
                    )
            }
        )

    result.sort(
        key=lambda x: x[
            "currentSessions"
        ],
        reverse=True
    )

    return result

# =========================================================
# Request Models
# =========================================================

class DashboardSummaryRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    channelLimit: int = Field(default=10, ge=1, le=50)
    
class ChannelReportRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(
        default=30,
        ge=1,
        le=365
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=100
    )


class PageFlowRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: Literal[
        "pageTitle",
        "pagePath"
    ] = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class PageFlowFromPageRequest(BaseModel):
    sourcePage: str
    matchType: Literal[
        "contains",
        "exact"
    ] = "contains"
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: Literal[
        "pageTitle",
        "pagePath"
    ] = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class PreviousPageRequest(BaseModel):
    targetPage: str
    matchType: Literal[
        "url",
        "title",
        "path"
    ] = "url"
    filterMatchType: Literal[
        "contains",
        "exact"
    ] = "contains"
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    displayDimension: Literal[
        "pageTitle",
        "pagePath"
    ] = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class ConversionPagesRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    eventName: str = "generate_lead"
    displayDimension: Literal[
        "pageTitle",
        "pagePath"
    ] = "pageTitle"
    limit: int = Field(default=50, ge=1, le=100)


class ConversionPathRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    eventName: str = "generate_lead"
    displayDimension: Literal[
        "pageTitle",
        "pagePath"
    ] = "pageTitle"
    limit: int = Field(default=50, ge=1, le=100)

class LandingPageConversionRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(
        default=30,
        ge=1,
        le=365
    )
    eventName: str = "generate_lead"
    limit: int = Field(
        default=100,
        ge=1,
        le=500
    )

class CampaignReportRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(
        default=30,
        ge=1,
        le=365
    )
    eventName: str = "generate_lead"
    limit: int = Field(
        default=100,
        ge=1,
        le=500
    )

class PagePerformanceRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(
        default=30,
        ge=1,
        le=365
    )
    eventName: str = "generate_lead"
    limit: int = Field(
        default=100,
        ge=1,
        le=500
    )

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
    displayDimension: Literal[
        "pageTitle",
        "pagePath"
    ] = "pageTitle"
    limit: int = Field(default=20, ge=1, le=100)


class ColumnRankingRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = Field(default=30, ge=1, le=365)
    limit: int = Field(default=20, ge=1, le=100)

class KeywordSearchVolumeRequest(BaseModel):
    keywords: list[str]
    languageId: str = "1005"
    geoTargetConstant: str = "geoTargetConstants/20636"

# =========================================================
# BigQuery Request Models
# =========================================================

class UsersByPageRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limit: int = Field(default=20, ge=1, le=100)
    matchType: Literal[
        "contains",
        "exact"
    ] = "contains"


class UserPathRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limitUsers: int = Field(default=20, ge=1, le=100)
    stepsPerUser: int = Field(default=10, ge=1, le=20)
    matchType: Literal[
        "contains",
        "exact"
    ] = "contains"


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
    matchType: Literal[
        "contains",
        "exact"
    ] = "contains"


class ConversionPrePagesRequest(BaseModel):
    targetPage: str
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    limitUsers: int = Field(default=50, ge=1, le=100)
    stepsPerUser: int = Field(default=5, ge=1, le=10)
    matchType: Literal[
        "contains",
        "exact"
    ] = "contains"

    excludePages: list[str] = Field(
        default_factory=list
    )

class ContentConversionContributionRequest(BaseModel):
    targetPage: str
    conversionPage: str = "/contact/thanks/"
    startDate: Optional[str] = None
    endDate: Optional[str] = None

    matchType: Literal[
        "contains",
        "exact"
    ] = "contains"

    limitUsers: int = Field(
        default=100,
        ge=1,
        le=500
    )
    
# =========================================================
# Google Ads: Keyword Search Volume
# =========================================================

@app.post("/api/google-ads/keyword/search-volume")
def keyword_search_volume(
    req: KeywordSearchVolumeRequest
):
    try:
        client = get_google_ads_client()

        service = client.get_service(
            "KeywordPlanIdeaService"
        )

        request = client.get_type(
            "GenerateKeywordHistoricalMetricsRequest"
        )

        request.customer_id = (
            GOOGLE_ADS_CUSTOMER_ID.replace("-", "")
        )

        request.keywords.extend(
            req.keywords
        )

        request.language = (
            f"languageConstants/{req.languageId}"
        )

        request.geo_target_constants.append(
            req.geoTargetConstant
        )

        request.keyword_plan_network = (
            client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
        )

        response = (
            service.generate_keyword_historical_metrics(
                request=request
            )
        )

        rows = []

        for result in response.results:
            metrics = result.keyword_metrics

            monthly = []

            for item in metrics.monthly_search_volumes:
                monthly.append(
                    {
                        "year": item.year.name,
                        "month": item.month.name,
                        "monthlySearches":
                            item.monthly_searches
                    }
                )

            rows.append(
                {
                    "keyword":
                        result.text,

                    "avgMonthlySearches":
                        metrics.avg_monthly_searches,

                    "competition":
                        metrics.competition.name,

                    "competitionIndex":
                        metrics.competition_index,

                    "lowTopOfPageBidMicros":
                        metrics.low_top_of_page_bid_micros,

                    "highTopOfPageBidMicros":
                        metrics.high_top_of_page_bid_micros,

                    "monthlySearchVolumes":
                        monthly
                }
            )

        return {
            "count": len(rows),
            "rows": rows
        }

    except HTTPException:
        raise

    except Exception as e:
        print(
            "=== GOOGLE ADS KEYWORD ERROR ==="
        )
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "Google Ads keyword search volume failed: "
                f"{str(e)}"
            )
        )

# =========================================================
# Search Console Request Models
# =========================================================

class SearchConsoleKeywordsRequest(BaseModel):
    startDate: str
    endDate: str
    rowLimit: int = Field(
        default=100,
        ge=1,
        le=1000
    )
    siteUrl: str = "sc-domain:ntecj.co.jp"

class SearchConsolePagesRequest(BaseModel):
    startDate: str
    endDate: str
    rowLimit: int = Field(
        default=100,
        ge=1,
        le=1000
    )
    siteUrl: str = "sc-domain:ntecj.co.jp"

class SeoOpportunityRequest(BaseModel):
    startDate: str
    endDate: str

    minImpressions: int = Field(
        default=100,
        ge=1
    )

    minPosition: float = Field(
        default=4.0,
        ge=1.0
    )

    maxPosition: float = Field(
        default=10.0,
        ge=1.0
    )

    maxCtr: float = Field(
        default=0.03,
        ge=0.0,
        le=1.0
    )

    rowLimit: int = Field(
        default=500,
        ge=1,
        le=1000
    )

    siteUrl: str = "sc-domain:ntecj.co.jp"

class SearchConsoleQueryRequest(BaseModel):
    query: str
    matchType: Literal[
        "exact",
        "contains"
    ] = "exact"

    startDate: str
    endDate: str

    rowLimit: int = Field(
        default=100,
        ge=1,
        le=1000
    )

    siteUrl: str = "sc-domain:ntecj.co.jp"

# =========================================================
# Root / Health
# =========================================================

@app.get("/")
def root():
    return {
        "service": "NTEC Analytics API",
        "status": "ok"
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "20260819-1"
    }


# =========================================================
# GA4: Column Ranking
# =========================================================

@app.post("/api/ga4/column/ranking")
def column_ranking(
    req: ColumnRankingRequest
):
    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "pagePath"
            },
            {
                "name": "pageTitle"
            }
        ],
        "metrics": [
            {
                "name": "screenPageViews"
            },
            {
                "name": "totalUsers"
            },
            {
                "name": "sessions"
            },
            {
                "name": "engagedSessions"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name="pagePath",
                value="/column/",
                match_type="CONTAINS"
            ),
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "screenPageViews"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)

# =========================================================
# GA4: Channel Report
# =========================================================

@app.post("/api/ga4/standard/channel")
def channel_report(
    req: ChannelReportRequest
):
    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name":
                    "sessionDefaultChannelGroup"
            }
        ],
        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            }
        ],
        "orderBys": [
            {
                "metric": {
                    "metricName": "sessions"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =========================================================
# GA4: Page Flow
# =========================================================

@app.post("/api/ga4/page/flow")
def page_flow(
    req: PageFlowRequest
):
    display_dimension = (
        get_display_dimension(
            req.displayDimension
        )
    )

    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "pageReferrer"
            },
            {
                "name": display_dimension
            }
        ],
        "metrics": [
            {
                "name": "screenPageViews"
            }
        ],
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "screenPageViews"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =========================================================
# GA4: Page Flow From Specific Page
# =========================================================

@app.post("/api/ga4/page/flow/from-page")
def page_flow_from_page(
    req: PageFlowFromPageRequest
):
    display_dimension = (
        get_display_dimension(
            req.displayDimension
        )
    )

    ga4_match_type = (
        get_ga4_match_type(
            req.matchType
        )
    )

    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "pageReferrer"
            },
            {
                "name": display_dimension
            }
        ],
        "metrics": [
            {
                "name": "screenPageViews"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name="pageReferrer",
                value=req.sourcePage,
                match_type=ga4_match_type
            ),
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "screenPageViews"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =========================================================
# GA4: Previous Pages
# =========================================================

@app.post("/api/ga4/page/before-page")
def previous_page(
    req: PreviousPageRequest
):
    match_field = get_match_field(
        req.matchType
    )

    display_dimension = (
        get_display_dimension(
            req.displayDimension
        )
    )

    ga4_match_type = (
        get_ga4_match_type(
            req.filterMatchType
        )
    )

    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "pageReferrer"
            },
            {
                "name": match_field
            },
            {
                "name": display_dimension
            }
        ],
        "metrics": [
            {
                "name": "screenPageViews"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name=match_field,
                value=req.targetPage,
                match_type=ga4_match_type
            ),
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "screenPageViews"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =========================================================
# GA4: Conversion Pages
# =========================================================

@app.post("/api/ga4/conversion/pages")
def conversion_pages(
    req: ConversionPagesRequest
):
    display_dimension = (
        get_display_dimension(
            req.displayDimension
        )
    )

    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "eventName"
            },
            {
                "name": display_dimension
            }
        ],
        "metrics": [
            {
                "name": "eventCount"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name="eventName",
                value=req.eventName,
                match_type="EXACT"
            ),
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "eventCount"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =========================================================
# GA4: Conversion Path
# =========================================================

@app.post("/api/ga4/conversion/path")
def conversion_path(
    req: ConversionPathRequest
):
    display_dimension = (
        get_display_dimension(
            req.displayDimension
        )
    )

    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "landingPage"
            },
            {
                "name": display_dimension
            }
        ],
        "metrics": [
            {
                "name": "eventCount"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name="eventName",
                value=req.eventName,
                match_type="EXACT"
            ),
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "eventCount"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)

# =========================================================
# GA4: Landing Page Conversion
# =========================================================

@app.post(
    "/api/ga4/conversion/landing-pages"
)
def landing_page_conversion(
    req: LandingPageConversionRequest
):
    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),

        "dimensions": [
            {
                "name": "landingPage"
            },
            {
                "name": "sessionDefaultChannelGroup"
            }
        ],

        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            },
            {
                "name": "eventCount"
            }
        ],

        "dimensionFilter":
            build_string_filter(
                field_name="eventName",
                value=req.eventName,
                match_type="EXACT"
            ),

        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "eventCount"
                },
                "desc": True
            }
        ],

        "limit": build_limit(
            req.limit
        )
    }

    return call_ga4(body)


# =========================================================
# GA4: Campaign Report
# =========================================================

@app.post(
    "/api/ga4/acquisition/campaigns"
)
def campaign_report(
    req: CampaignReportRequest
):
    date_ranges = build_date_ranges(
        req.startDate,
        req.endDate,
        req.days
    )

    # -----------------------------------------------------
    # Traffic
    # -----------------------------------------------------

    traffic_body = {
        "dateRanges": date_ranges,

        "dimensions": [
            {
                "name": "sessionSource"
            },
            {
                "name": "sessionMedium"
            },
            {
                "name": "sessionCampaignName"
            }
        ],

        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            }
        ],

        "orderBys": [
            {
                "metric": {
                    "metricName": "sessions"
                },
                "desc": True
            }
        ],

        "limit": build_limit(
            req.limit
        )
    }

    traffic_response = call_ga4(
        traffic_body
    )

    # -----------------------------------------------------
    # Conversions
    # -----------------------------------------------------

    conversion_body = {
        "dateRanges": date_ranges,

        "dimensions": [
            {
                "name": "sessionSource"
            },
            {
                "name": "sessionMedium"
            },
            {
                "name": "sessionCampaignName"
            }
        ],

        "metrics": [
            {
                "name": "eventCount"
            }
        ],

        "dimensionFilter":
            build_string_filter(
                field_name="eventName",
                value=req.eventName,
                match_type="EXACT"
            ),

        "limit": build_limit(
            req.limit
        )
    }

    conversion_response = call_ga4(
        conversion_body
    )

    # -----------------------------------------------------
    # Build Conversion Map
    # -----------------------------------------------------

    conversion_map = {}

    for row in conversion_response.get(
        "rows",
        []
    ):
        dimensions = row.get(
            "dimensionValues",
            []
        )

        metrics = row.get(
            "metricValues",
            []
        )

        source = (
            dimensions[0].get("value", "")
            if len(dimensions) > 0
            else ""
        )

        medium = (
            dimensions[1].get("value", "")
            if len(dimensions) > 1
            else ""
        )

        campaign = (
            dimensions[2].get("value", "")
            if len(dimensions) > 2
            else ""
        )

        conversions = (
            safe_int(
                metrics[0].get("value", 0)
            )
            if metrics
            else 0
        )

        conversion_map[
            (
                source,
                medium,
                campaign
            )
        ] = conversions

    # -----------------------------------------------------
    # Merge
    # -----------------------------------------------------

    rows = []

    for row in traffic_response.get(
        "rows",
        []
    ):
        dimensions = row.get(
            "dimensionValues",
            []
        )

        metrics = row.get(
            "metricValues",
            []
        )

        source = (
            dimensions[0].get("value", "")
            if len(dimensions) > 0
            else ""
        )

        medium = (
            dimensions[1].get("value", "")
            if len(dimensions) > 1
            else ""
        )

        campaign = (
            dimensions[2].get("value", "")
            if len(dimensions) > 2
            else ""
        )

        sessions = (
            safe_int(
                metrics[0].get("value", 0)
            )
            if len(metrics) > 0
            else 0
        )

        users = (
            safe_int(
                metrics[1].get("value", 0)
            )
            if len(metrics) > 1
            else 0
        )

        conversions = conversion_map.get(
            (
                source,
                medium,
                campaign
            ),
            0
        )

        conversion_rate = (
            round(
                conversions
                / sessions
                * 100,
                2
            )
            if sessions > 0
            else 0
        )

        rows.append(
            {
                "source": source,
                "medium": medium,
                "campaign": campaign,
                "sessions": sessions,
                "users": users,
                "conversions": conversions,
                "conversionRate":
                    conversion_rate
            }
        )

    return {
        "eventName": req.eventName,
        "count": len(rows),
        "rows": rows
    }

# =========================================================
# GA4: Page Performance Report
# =========================================================

@app.post(
    "/api/ga4/page/performance"
)
def page_performance_report(
    req: PagePerformanceRequest
):
    date_ranges = build_date_ranges(
        req.startDate,
        req.endDate,
        req.days
    )

    # -----------------------------------------------------
    # Page performance
    # -----------------------------------------------------

    performance_body = {
        "dateRanges": date_ranges,

        "dimensions": [
            {
                "name": "pagePath"
            },
            {
                "name": "pageTitle"
            }
        ],

        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            },
            {
                "name": "screenPageViews"
            },
            {
                "name": "engagedSessions"
            },
            {
                "name": "bounceRate"
            }
        ],

        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "screenPageViews"
                },
                "desc": True
            }
        ],

        "limit": build_limit(
            req.limit
        )
    }

    performance_response = call_ga4(
        performance_body
    )

    # -----------------------------------------------------
    # Conversions by page
    # -----------------------------------------------------

    conversion_body = {
        "dateRanges": date_ranges,

        "dimensions": [
            {
                "name": "pagePath"
            }
        ],

        "metrics": [
            {
                "name": "eventCount"
            }
        ],

        "dimensionFilter":
            build_string_filter(
                field_name="eventName",
                value=req.eventName,
                match_type="EXACT"
            ),

        "limit": build_limit(
            req.limit
        )
    }

    conversion_response = call_ga4(
        conversion_body
    )

    conversion_map = {}

    for row in conversion_response.get(
        "rows",
        []
    ):
        dimensions = row.get(
            "dimensionValues",
            []
        )

        metrics = row.get(
            "metricValues",
            []
        )

        page_path = (
            dimensions[0].get(
                "value",
                ""
            )
            if dimensions
            else ""
        )
        
if page_path != "/":
page_path = page_path.rstrip("/")

        conversions = (
            safe_int(
                metrics[0].get(
                    "value",
                    0
                )
            )
            if metrics
            else 0
        )

        conversion_map[
            page_path
        ] = conversions

    rows = []

    for row in performance_response.get(
        "rows",
        []
    ):
        dimensions = row.get(
            "dimensionValues",
            []
        )

        metrics = row.get(
            "metricValues",
            []
        )

        page_path = (
            dimensions[0].get(
                "value",
                ""
            )
            if len(dimensions) > 0
            else ""
        )

        page_title = (
            dimensions[1].get(
                "value",
                ""
            )
            if len(dimensions) > 1
            else ""
        )

        sessions = (
            safe_int(
                metrics[0].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 0
            else 0
        )

        users = (
            safe_int(
                metrics[1].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 1
            else 0
        )

        page_views = (
            safe_int(
                metrics[2].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 2
            else 0
        )

        engaged_sessions = (
            safe_int(
                metrics[3].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 3
            else 0
        )

        bounce_rate = (
            safe_float(
                metrics[4].get(
                    "value",
                    0
                )
            )
            if len(metrics) > 4
            else 0
        )

        conversions = conversion_map.get(
            page_path,
            0
        )

        conversion_rate = (
            round(
                conversions
                / sessions
                * 100,
                2
            )
            if sessions > 0
            else 0
        )

        engagement_rate = (
            round(
                engaged_sessions
                / sessions
                * 100,
                2
            )
            if sessions > 0
            else 0
        )

        rows.append(
            {
                "pagePath":
                    page_path,

                "pageTitle":
                    page_title,

                "sessions":
                    sessions,

                "users":
                    users,

                "pageViews":
                    page_views,

                "engagedSessions":
                    engaged_sessions,

                "engagementRate":
                    engagement_rate,

                "bounceRate":
                    round(
                        bounce_rate * 100,
                        2
                    ),

                "conversions":
                    conversions,

                "conversionRate":
                    conversion_rate
            }
        )

    return {
        "eventName":
            req.eventName,

        "count":
            len(rows),

        "rows":
            rows
    }

# =========================================================
# GA4: Conversion Summary
# =========================================================

@app.post("/api/ga4/conversion/summary")
def conversion_summary(
    req: ConversionSummaryRequest
):
    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "eventName"
            }
        ],
        "metrics": [
            {
                "name": "eventCount"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name="eventName",
                value=req.eventName,
                match_type="EXACT"
            )
    }

    return call_ga4(body)


# =========================================================
# GA4: Thanks Page
# =========================================================

@app.post(
    "/api/ga4/conversion/thanks-summary"
)
def thanks_summary(
    req: ThanksPageSummaryRequest
):
    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": "pagePath"
            }
        ],
        "metrics": [
            {
                "name": "screenPageViews"
            },
            {
                "name": "sessions"
            }
        ],
        "dimensionFilter":
            build_string_filter(
                field_name="pagePath",
                value=req.thanksPage,
                match_type="EXACT"
            )
    }

    return call_ga4(body)


# =========================================================
# GA4: Exit Pages
# =========================================================

@app.post("/api/ga4/page/exits")
def page_exits(
    req: ExitPagesRequest
):
    display_dimension = (
        get_display_dimension(
            req.displayDimension
        )
    )

    body = {
        "dateRanges": build_date_ranges(
            req.startDate,
            req.endDate,
            req.days
        ),
        "dimensions": [
            {
                "name": display_dimension
            }
        ],
        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "screenPageViews"
            },
            {
                "name": "bounceRate"
            }
        ],
        "orderBys": [
            {
                "metric": {
                    "metricName":
                        "bounceRate"
                },
                "desc": True
            }
        ],
        "limit": build_limit(req.limit)
    }

    return call_ga4(body)


# =========================================================
# BigQuery: Users by Page
# =========================================================

@app.post("/api/bq/page/users")
def bq_users_by_page(
    req: UsersByPageRequest
):
    date_condition, date_params = (
        build_bq_date_condition(
            req.startDate,
            req.endDate
        )
    )

    if req.matchType == "exact":

        page_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """

        page_params = [
            bigquery.ScalarQueryParameter(
                "targetPage",
                "STRING",
                req.targetPage
            )
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
            bigquery.ScalarQueryParameter(
                "targetPageLike",
                "STRING",
                f"%{req.targetPage}%"
            )
        ]

    sql = f"""
    SELECT
      user_pseudo_id,
      COUNT(*) AS page_views,
      MIN(
        TIMESTAMP_MICROS(event_timestamp)
      ) AS first_seen,
      MAX(
        TIMESTAMP_MICROS(event_timestamp)
      ) AS last_seen
    FROM
      `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
    WHERE
      {date_condition}
      AND event_name = 'page_view'
      AND {page_condition}
    GROUP BY
      user_pseudo_id
    ORDER BY
      page_views DESC,
      last_seen DESC
    LIMIT @limit
    """

    params = (
        date_params
        + page_params
        + [
            bigquery.ScalarQueryParameter(
                "limit",
                "INT64",
                req.limit
            )
        ]
    )

    rows = run_bq_query(
        sql,
        params
    )

    return {
        "count": len(rows),
        "rows": [
            {
                "userPseudoId":
                    row["user_pseudo_id"],

                "pageViews":
                    row["page_views"],

                "firstSeen":
                    (
                        row["first_seen"].isoformat()
                        if row["first_seen"]
                        else None
                    ),

                "lastSeen":
                    (
                        row["last_seen"].isoformat()
                        if row["last_seen"]
                        else None
                    )
            }
            for row in rows
        ]
    }


# =========================================================
# BigQuery: User Paths by Target Page
# =========================================================

@app.post("/api/bq/user/path")
def bq_user_path(
    req: UserPathRequest
):
    date_condition, date_params = (
        build_bq_date_condition(
            req.startDate,
            req.endDate
        )
    )

    if req.matchType == "exact":

        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPage",
                "STRING",
                req.targetPage
            )
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
            bigquery.ScalarQueryParameter(
                "targetPageLike",
                "STRING",
                f"%{req.targetPage}%"
            )
        ]

    sql = f"""
    WITH target_hits AS (
      SELECT
        user_pseudo_id,
        MAX(
          TIMESTAMP_MICROS(event_timestamp)
        ) AS latest_target_time
      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`
      WHERE
        {date_condition}
        AND event_name = 'page_view'
        AND {target_condition}
      GROUP BY
        user_pseudo_id
    ),

    target_users AS (
      SELECT
        user_pseudo_id,
        latest_target_time
      FROM
        target_hits
      ORDER BY
        latest_target_time DESC
      LIMIT @limitUsers
    ),

    page_events AS (
      SELECT
        e.user_pseudo_id,

        TIMESTAMP_MICROS(
          e.event_timestamp
        ) AS event_time,

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

      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*` e

      INNER JOIN
        target_users tu

      ON
        e.user_pseudo_id
        = tu.user_pseudo_id

      WHERE
        {
            date_condition.replace(
                "_TABLE_SUFFIX",
                "e._TABLE_SUFFIX"
            )
        }

        AND e.event_name = 'page_view'
    ),

    ranked AS (
      SELECT
        *,

        ROW_NUMBER() OVER (
          PARTITION BY
            user_pseudo_id

          ORDER BY
            event_time DESC
        ) AS rn

      FROM
        page_events
    )

    SELECT
      user_pseudo_id,
      ga_session_id,
      event_time,
      page_location,
      page_title

    FROM
      ranked

    WHERE
      rn <= @stepsPerUser

    ORDER BY
      user_pseudo_id,
      event_time ASC
    """

    params = (
        date_params
        + target_params
        + [
            bigquery.ScalarQueryParameter(
                "limitUsers",
                "INT64",
                req.limitUsers
            ),
            bigquery.ScalarQueryParameter(
                "stepsPerUser",
                "INT64",
                req.stepsPerUser
            )
        ]
    )

    rows = run_bq_query(
        sql,
        params
    )

    grouped = {}

    for row in rows:

        user_id = (
            row["user_pseudo_id"]
        )

        if user_id not in grouped:
            grouped[user_id] = []

        grouped[user_id].append(
            {
                "sessionId":
                    row["ga_session_id"],

                "eventTime":
                    (
                        row["event_time"].isoformat()
                        if row["event_time"]
                        else None
                    ),

                "pageLocation":
                    row["page_location"],

                "pageTitle":
                    row["page_title"]
            }
        )

    return {
        "count": len(grouped),
        "rows": [
            {
                "userPseudoId": user_id,
                "journey": journey
            }
            for user_id, journey
            in grouped.items()
        ]
    }


# =========================================================
# BigQuery: Single User Journey
# =========================================================

@app.post("/api/bq/user/journey")
def bq_single_user_journey(
    req: UserJourneyRequest
):
    date_condition, date_params = (
        build_bq_date_condition(
            req.startDate,
            req.endDate
        )
    )

    sql = f"""
    SELECT
      user_pseudo_id,

      TIMESTAMP_MICROS(
        event_timestamp
      ) AS event_time,

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

    FROM
      `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`

    WHERE
      {date_condition}

      AND event_name = 'page_view'

      AND user_pseudo_id
          = @userPseudoId

    ORDER BY
      event_time ASC

    LIMIT
      @limit
    """

    params = (
        date_params
        + [
            bigquery.ScalarQueryParameter(
                "userPseudoId",
                "STRING",
                req.userPseudoId
            ),

            bigquery.ScalarQueryParameter(
                "limit",
                "INT64",
                req.limit
            )
        ]
    )

    rows = run_bq_query(
        sql,
        params
    )

    return {
        "count": len(rows),
        "rows": [
            {
                "userPseudoId":
                    row["user_pseudo_id"],

                "sessionId":
                    row["ga_session_id"],

                "eventTime":
                    (
                        row["event_time"].isoformat()
                        if row["event_time"]
                        else None
                    ),

                "pageLocation":
                    row["page_location"],

                "pageTitle":
                    row["page_title"]
            }
            for row in rows
        ]
    }


# =========================================================
# BigQuery: Pre Pages Before Target
# =========================================================

@app.post("/api/bq/page/pre-pages")
def bq_pre_pages_before_target(
    req: PrePagesBeforeTargetRequest
):
    date_condition, date_params = (
        build_bq_date_condition(
            req.startDate,
            req.endDate
        )
    )

    if req.matchType == "exact":

        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """

        exclude_target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) != @targetPage
        """

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPage",
                "STRING",
                req.targetPage
            )
        ]

    else:

        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
        """

        exclude_target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) NOT LIKE @targetPageLike
        """

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPageLike",
                "STRING",
                f"%{req.targetPage}%"
            )
        ]

    sql = f"""
    WITH target_hits AS (
      SELECT
        user_pseudo_id,

        TIMESTAMP_MICROS(
          event_timestamp
        ) AS target_time

      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`

      WHERE
        {date_condition}

        AND event_name = 'page_view'

        AND {target_condition}
    ),

    target_users AS (
      SELECT
        user_pseudo_id,

        MAX(
          target_time
        ) AS latest_target_time

      FROM
        target_hits

      GROUP BY
        user_pseudo_id

      ORDER BY
        latest_target_time DESC

      LIMIT
        @limitUsers
    ),

    page_events AS (
      SELECT
        e.user_pseudo_id,

        TIMESTAMP_MICROS(
          e.event_timestamp
        ) AS event_time,

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

      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*` e

      INNER JOIN
        target_users tu

      ON
        e.user_pseudo_id
        = tu.user_pseudo_id

      WHERE
        {
            date_condition.replace(
                "_TABLE_SUFFIX",
                "e._TABLE_SUFFIX"
            )
        }

        AND e.event_name = 'page_view'

        AND TIMESTAMP_MICROS(
            e.event_timestamp
        ) < tu.latest_target_time

        AND {exclude_target_condition}
    ),

    ranked AS (
      SELECT
        *,

        ROW_NUMBER() OVER (
          PARTITION BY
            user_pseudo_id

          ORDER BY
            event_time DESC
        ) AS rn_desc

      FROM
        page_events
    )

    SELECT
      user_pseudo_id,
      event_time,
      page_location,
      page_title

    FROM
      ranked

    WHERE
      rn_desc <= @stepsPerUser

    ORDER BY
      user_pseudo_id,
      event_time ASC
    """

    params = (
        date_params
        + target_params
        + [
            bigquery.ScalarQueryParameter(
                "limitUsers",
                "INT64",
                req.limitUsers
            ),

            bigquery.ScalarQueryParameter(
                "stepsPerUser",
                "INT64",
                req.stepsPerUser
            )
        ]
    )

    rows = run_bq_query(
        sql,
        params
    )

    grouped = {}

    for row in rows:

        user_id = (
            row["user_pseudo_id"]
        )

        if user_id not in grouped:
            grouped[user_id] = []

        grouped[user_id].append(
            {
                "eventTime":
                    (
                        row["event_time"].isoformat()
                        if row["event_time"]
                        else None
                    ),

                "pageLocation":
                    row["page_location"],

                "pageTitle":
                    row["page_title"]
            }
        )

    return {
        "count": len(grouped),
        "rows": [
            {
                "userPseudoId":
                    user_id,

                "prePages":
                    pages
            }
            for user_id, pages
            in grouped.items()
        ]
    }


# =========================================================
# BigQuery: Conversion Pre Pages
# =========================================================

@app.post(
    "/api/bq/conversion/pre-pages"
)
def bq_conversion_pre_pages(
    req: ConversionPrePagesRequest
):
    date_condition, date_params = (
        build_bq_date_condition(
            req.startDate,
            req.endDate
        )
    )

    if req.matchType == "exact":

        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) = @targetPage
        """

        exclude_target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) != @targetPage
        """

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPage",
                "STRING",
                req.targetPage
            )
        ]

    else:

        target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) LIKE @targetPageLike
        """

        exclude_target_condition = """
        (
          SELECT ep.value.string_value
          FROM UNNEST(e.event_params) ep
          WHERE ep.key = 'page_location'
        ) NOT LIKE @targetPageLike
        """

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPageLike",
                "STRING",
                f"%{req.targetPage}%"
            )
        ]

    (
        exclude_condition,
        exclude_params
    ) = build_bq_exclude_conditions(
        req.excludePages,
        alias="e"
    )

    sql = f"""
    WITH target_hits AS (
      SELECT
        user_pseudo_id,

        TIMESTAMP_MICROS(
          event_timestamp
        ) AS target_time

      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`

      WHERE
        {date_condition}

        AND event_name = 'page_view'

        AND {target_condition}
    ),

    latest_target_per_user AS (
      SELECT
        user_pseudo_id,

        MAX(
          target_time
        ) AS latest_target_time

      FROM
        target_hits

      GROUP BY
        user_pseudo_id

      ORDER BY
        latest_target_time DESC

      LIMIT
        @limitUsers
    ),

    page_events AS (
      SELECT
        e.user_pseudo_id,

        TIMESTAMP_MICROS(
          e.event_timestamp
        ) AS event_time,

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

      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*` e

      INNER JOIN
        latest_target_per_user t

      ON
        e.user_pseudo_id
        = t.user_pseudo_id

      WHERE
        {
            date_condition.replace(
                "_TABLE_SUFFIX",
                "e._TABLE_SUFFIX"
            )
        }

        AND e.event_name = 'page_view'

        AND TIMESTAMP_MICROS(
            e.event_timestamp
        ) < t.latest_target_time

        AND {exclude_target_condition}

        {exclude_condition}
    ),

    ranked AS (
      SELECT
        *,

        ROW_NUMBER() OVER (
          PARTITION BY
            user_pseudo_id

          ORDER BY
            event_time DESC
        ) AS rn_desc

      FROM
        page_events
    )

    SELECT
      page_location,
      page_title,

      COUNT(*) AS appearance_count,

      COUNT(
        DISTINCT user_pseudo_id
      ) AS users_count

    FROM
      ranked

    WHERE
      rn_desc <= @stepsPerUser

    GROUP BY
      page_location,
      page_title

    ORDER BY
      users_count DESC,
      appearance_count DESC

    LIMIT 100
    """

    params = (
        date_params
        + target_params
        + exclude_params
        + [
            bigquery.ScalarQueryParameter(
                "limitUsers",
                "INT64",
                req.limitUsers
            ),

            bigquery.ScalarQueryParameter(
                "stepsPerUser",
                "INT64",
                req.stepsPerUser
            )
        ]
    )

    rows = run_bq_query(
        sql,
        params
    )

    return {
        "count": len(rows),

        "rows": [
            {
                "pageLocation":
                    row["page_location"],

                "pageTitle":
                    row["page_title"],

                "appearanceCount":
                    row["appearance_count"],

                "usersCount":
                    row["users_count"]
            }
            for row in rows
        ]
    }

# =========================================================
# BigQuery: Content Conversion Contribution
# =========================================================

@app.post(
    "/api/bq/content/conversion-contribution"
)
def bq_content_conversion_contribution(
    req: ContentConversionContributionRequest
):
    date_condition, date_params = (
        build_bq_date_condition(
            req.startDate,
            req.endDate
        )
    )

    if req.matchType == "exact":
        target_condition = (
            "page_location = @targetPage"
        )

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPage",
                "STRING",
                req.targetPage
            )
        ]

    else:
        target_condition = (
            "page_location LIKE @targetPageLike"
        )

        target_params = [
            bigquery.ScalarQueryParameter(
                "targetPageLike",
                "STRING",
                f"%{req.targetPage}%"
            )
        ]

    sql = f"""
    WITH page_events AS (
      SELECT
        user_pseudo_id,

        TIMESTAMP_MICROS(
          event_timestamp
        ) AS event_time,

        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params) ep
          WHERE ep.key = 'page_location'
        ) AS page_location

      FROM
        `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.events_*`

      WHERE
        {date_condition}

        AND event_name = 'page_view'
    ),

    target_hits AS (
      SELECT
        user_pseudo_id,

        MIN(event_time) AS first_target_time,

        COUNT(*) AS target_page_views

      FROM
        page_events

      WHERE
        {target_condition}

      GROUP BY
        user_pseudo_id
    ),

    contribution AS (
      SELECT
        t.user_pseudo_id,

        t.first_target_time,

        t.target_page_views,

        COUNTIF(
          p.page_location LIKE @conversionPageLike
          AND p.event_time > t.first_target_time
        ) AS conversions_after_content,

        MIN(
          IF(
            p.page_location LIKE @conversionPageLike
            AND p.event_time > t.first_target_time,
            p.event_time,
            NULL
          )
        ) AS first_conversion_time

      FROM
        target_hits t

      LEFT JOIN
        page_events p

      ON
        t.user_pseudo_id
        = p.user_pseudo_id

      GROUP BY
        t.user_pseudo_id,
        t.first_target_time,
        t.target_page_views
    ),

    final AS (
      SELECT
        *,

        COUNT(*) OVER() AS target_users,

        COUNTIF(
          conversions_after_content > 0
        ) OVER() AS converted_users,

        SUM(
          conversions_after_content
        ) OVER() AS total_conversions_after_content

      FROM
        contribution
    )

    SELECT
      *

    FROM
      final

    ORDER BY
      first_conversion_time DESC,
      first_target_time DESC

    LIMIT
      @limitUsers
    """

    params = (
        date_params
        + target_params
        + [
            bigquery.ScalarQueryParameter(
                "conversionPageLike",
                "STRING",
                f"%{req.conversionPage}%"
            ),

            bigquery.ScalarQueryParameter(
                "limitUsers",
                "INT64",
                req.limitUsers
            )
        ]
    )

    rows = run_bq_query(
        sql,
        params
    )

    if not rows:
        return {
            "targetPage": req.targetPage,
            "conversionPage":
                req.conversionPage,
            "targetUsers": 0,
            "convertedUsers": 0,
            "conversionRate": 0,
            "conversionsAfterContent": 0,
            "rows": []
        }

    target_users = int(
        rows[0]["target_users"]
    )

    converted_users = int(
        rows[0]["converted_users"]
    )

    total_conversions = int(
        rows[0][
            "total_conversions_after_content"
        ] or 0
    )

    conversion_rate = (
        round(
            converted_users
            / target_users
            * 100,
            2
        )
        if target_users > 0
        else 0
    )

    return {
        "targetPage": req.targetPage,

        "conversionPage":
            req.conversionPage,

        "targetUsers":
            target_users,

        "convertedUsers":
            converted_users,

        "conversionRate":
            conversion_rate,

        "conversionsAfterContent":
            total_conversions,

        "rows": [
            {
                "userPseudoId":
                    row["user_pseudo_id"],

                "targetPageViews":
                    row["target_page_views"],

                "firstTargetTime":
                    (
                        row[
                            "first_target_time"
                        ].isoformat()
                        if row[
                            "first_target_time"
                        ]
                        else None
                    ),

                "conversionsAfterContent":
                    row[
                        "conversions_after_content"
                    ],

                "firstConversionTime":
                    (
                        row[
                            "first_conversion_time"
                        ].isoformat()
                        if row[
                            "first_conversion_time"
                        ]
                        else None
                    )
            }
            for row in rows
        ]
    }

# =========================================================
# Search Console: Sites
# =========================================================

@app.get("/api/search-console/sites")
def search_console_sites():

    service = (
        get_search_console_service()
    )

    try:
        response = (
            service
            .sites()
            .list()
            .execute()
        )

        return response

    except Exception as e:

        print(
            "=== SEARCH CONSOLE SITES ERROR ==="
        )
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "Search Console sites list failed: "
                f"{str(e)}"
            )
        )


# =========================================================
# Search Console: Keywords
# =========================================================

@app.post(
    "/api/search-console/keywords"
)
def search_console_keywords(
    req: SearchConsoleKeywordsRequest
):
    service = (
        get_search_console_service()
    )

    body = {
        "startDate": req.startDate,
        "endDate": req.endDate,
        "dimensions": [
            "query",
            "page"
        ],
        "rowLimit": req.rowLimit
    }

    try:
        response = (
            service
            .searchanalytics()
            .query(
                siteUrl=req.siteUrl,
                body=body
            )
            .execute()
        )

        return response

    except Exception as e:

        print(
            "=== SEARCH CONSOLE QUERY ERROR ==="
        )
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "Search Console query failed: "
                f"{str(e)}"
            )
        )

# =========================================================
# Search Console: Pages
# =========================================================

@app.post(
    "/api/search-console/pages"
)
def search_console_pages(
    req: SearchConsolePagesRequest
):
    service = get_search_console_service()

    body = {
        "startDate": req.startDate,
        "endDate": req.endDate,
        "dimensions": [
            "page"
        ],
        "rowLimit": req.rowLimit
    }

    try:
        response = (
            service
            .searchanalytics()
            .query(
                siteUrl=req.siteUrl,
                body=body
            )
            .execute()
        )

        rows = []

        for row in response.get(
            "rows",
            []
        ):
            keys = row.get(
                "keys",
                []
            )

            rows.append(
                {
                    "page":
                        keys[0]
                        if len(keys) > 0
                        else None,

                    "clicks":
                        row.get(
                            "clicks",
                            0
                        ),

                    "impressions":
                        row.get(
                            "impressions",
                            0
                        ),

                    "ctr":
                        row.get(
                            "ctr",
                            0
                        ),

                    "position":
                        row.get(
                            "position",
                            0
                        )
                }
            )

        return {
            "count": len(rows),
            "rows": rows
        }

    except Exception as e:

        print(
            "=== SEARCH CONSOLE PAGES ERROR ==="
        )
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "Search Console pages report failed: "
                f"{str(e)}"
            )
        )

# =========================================================
# Search Console: SEO Opportunities
# =========================================================

@app.post(
    "/api/search-console/seo-opportunities"
)
def search_console_seo_opportunities(
    req: SeoOpportunityRequest
):
    service = get_search_console_service()

    body = {
        "startDate": req.startDate,
        "endDate": req.endDate,
        "dimensions": [
            "page"
        ],
        "rowLimit": req.rowLimit
    }

    try:
        response = (
            service
            .searchanalytics()
            .query(
                siteUrl=req.siteUrl,
                body=body
            )
            .execute()
        )

        rows = []

        for row in response.get(
            "rows",
            []
        ):
            keys = row.get(
                "keys",
                []
            )

            page = (
                keys[0]
                if len(keys) > 0
                else None
            )

            clicks = row.get(
                "clicks",
                0
            )

            impressions = row.get(
                "impressions",
                0
            )

            ctr = row.get(
                "ctr",
                0
            )

            position = row.get(
                "position",
                0
            )

            if impressions < req.minImpressions:
                continue

            if position < req.minPosition:
                continue

            if position > req.maxPosition:
                continue

            if ctr > req.maxCtr:
                continue

            opportunity_score = round(
                impressions
                * max(
                    req.maxCtr - ctr,
                    0
                )
                * max(
                    req.maxPosition - position + 1,
                    1
                ),
                2
            )

            rows.append(
                {
                    "page": page,
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": ctr,
                    "position": position,
                    "opportunityScore":
                        opportunity_score
                }
            )

        rows.sort(
            key=lambda x:
                x["opportunityScore"],
            reverse=True
        )

        return {
            "filters": {
                "minImpressions":
                    req.minImpressions,

                "minPosition":
                    req.minPosition,

                "maxPosition":
                    req.maxPosition,

                "maxCtr":
                    req.maxCtr
            },

            "count": len(rows),

            "rows": rows
        }

    except Exception as e:

        print(
            "=== SEO OPPORTUNITY ERROR ==="
        )
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "SEO opportunity report failed: "
                f"{str(e)}"
            )
        )
# =========================================================
# Search Console: Query Filter
# =========================================================

@app.post(
    "/api/search-console/query"
)
def search_console_query(
    req: SearchConsoleQueryRequest
):
    service = (
        get_search_console_service()
    )

    operator = (
        "equals"
        if req.matchType == "exact"
        else "contains"
    )

    body = {
        "startDate": req.startDate,
        "endDate": req.endDate,

        "dimensions": [
            "query",
            "page"
        ],

        "dimensionFilterGroups": [
            {
                "groupType": "and",
                "filters": [
                    {
                        "dimension": "query",
                        "operator": operator,
                        "expression": req.query
                    }
                ]
            }
        ],

        "rowLimit": req.rowLimit
    }

    try:
        response = (
            service
            .searchanalytics()
            .query(
                siteUrl=req.siteUrl,
                body=body
            )
            .execute()
        )

        rows = []

        for row in response.get(
            "rows",
            []
        ):
            keys = row.get(
                "keys",
                []
            )

            rows.append(
                {
                    "query":
                        keys[0]
                        if len(keys) > 0
                        else None,

                    "page":
                        keys[1]
                        if len(keys) > 1
                        else None,

                    "clicks":
                        row.get(
                            "clicks",
                            0
                        ),

                    "impressions":
                        row.get(
                            "impressions",
                            0
                        ),

                    "ctr":
                        row.get(
                            "ctr",
                            0
                        ),

                    "position":
                        row.get(
                            "position",
                            0
                        )
                }
            )

        return {
            "query": req.query,
            "matchType": req.matchType,
            "count": len(rows),
            "rows": rows
        }

    except Exception as e:

        print(
            "=== SEARCH CONSOLE QUERY FILTER ERROR ==="
        )
        print(type(e).__name__)
        print(str(e))

        raise HTTPException(
            status_code=500,
            detail=(
                "Search Console query filter failed: "
                f"{str(e)}"
            )
        )

# =========================================================
# Dashboard: Pulse Summary
# =========================================================

@app.post("/api/dashboard/summary")
def dashboard_summary(
    req: DashboardSummaryRequest
):
    # -----------------------------------------------------
    # Current period
    # -----------------------------------------------------

    current_date_ranges = build_date_ranges(
        req.startDate,
        req.endDate,
        req.days
    )

    current_overview_body = {
        "dateRanges": current_date_ranges,
        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            },
            {
                "name": "screenPageViews"
            },
            {
                "name": "engagedSessions"
            }
        ]
    }

    current_overview = call_ga4(
        current_overview_body
    )

    # -----------------------------------------------------
    # Previous period
    # -----------------------------------------------------

    previous_period = calculate_previous_period(
        req.startDate,
        req.endDate,
        req.days
    )

    previous_overview_body = {
        "dateRanges": [
            previous_period
        ],
        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            },
            {
                "name": "screenPageViews"
            },
            {
                "name": "engagedSessions"
            }
        ]
    }

    previous_overview = call_ga4(
        previous_overview_body
    )

    # -----------------------------------------------------
    # Current metrics
    # -----------------------------------------------------

    current_sessions = safe_int(
        get_metric_value(
            current_overview,
            0
        )
    )

    current_users = safe_int(
        get_metric_value(
            current_overview,
            1
        )
    )

    current_pageviews = safe_int(
        get_metric_value(
            current_overview,
            2
        )
    )

    current_engaged = safe_int(
        get_metric_value(
            current_overview,
            3
        )
    )

    # -----------------------------------------------------
    # Previous metrics
    # -----------------------------------------------------

    previous_sessions = safe_int(
        get_metric_value(
            previous_overview,
            0
        )
    )

    previous_users = safe_int(
        get_metric_value(
            previous_overview,
            1
        )
    )

    previous_pageviews = safe_int(
        get_metric_value(
            previous_overview,
            2
        )
    )

    previous_engaged = safe_int(
        get_metric_value(
            previous_overview,
            3
        )
    )

    # -----------------------------------------------------
    # Changes
    # -----------------------------------------------------

    sessions_change = percent_change(
        current_sessions,
        previous_sessions
    )

    users_change = percent_change(
        current_users,
        previous_users
    )

    pageviews_change = percent_change(
        current_pageviews,
        previous_pageviews
    )

    engaged_change = percent_change(
        current_engaged,
        previous_engaged
    )

    # -----------------------------------------------------
    # KPI Object
    # -----------------------------------------------------

    kpis = {
        "sessions": {
            "label": "Sessions",
            "value": current_sessions,
            "previousValue": previous_sessions,
            "changePercent": sessions_change,
            "status": health_from_change(
                sessions_change
            ),
            "lineage": {
                "source": "GA4 Data API",
                "metric": "sessions",
                "endpoint": "/api/dashboard/summary"
            }
        },

        "users": {
            "label": "Users",
            "value": current_users,
            "previousValue": previous_users,
            "changePercent": users_change,
            "status": health_from_change(
                users_change
            ),
            "lineage": {
                "source": "GA4 Data API",
                "metric": "totalUsers",
                "endpoint": "/api/dashboard/summary"
            }
        },

        "pageViews": {
            "label": "Page Views",
            "value": current_pageviews,
            "previousValue": previous_pageviews,
            "changePercent": pageviews_change,
            "status": health_from_change(
                pageviews_change
            ),
            "lineage": {
                "source": "GA4 Data API",
                "metric": "screenPageViews",
                "endpoint": "/api/dashboard/summary"
            }
        },

        "engagedSessions": {
            "label": "Engaged Sessions",
            "value": current_engaged,
            "previousValue": previous_engaged,
            "changePercent": engaged_change,
            "status": health_from_change(
                engaged_change
            ),
            "lineage": {
                "source": "GA4 Data API",
                "metric": "engagedSessions",
                "endpoint": "/api/dashboard/summary"
            }
        }
    }

    # -----------------------------------------------------
    # Current Channel Drilldown
    # -----------------------------------------------------

    channel_body = {
        "dateRanges": current_date_ranges,

        "dimensions": [
            {
                "name": "sessionDefaultChannelGroup"
            }
        ],

        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            }
        ],

        "orderBys": [
            {
                "metric": {
                    "metricName": "sessions"
                },
                "desc": True
            }
        ],

        "limit": build_limit(
            req.channelLimit
        )
    }

    channel_response = call_ga4(
        channel_body
    )

    channels = extract_channel_rows(
        channel_response
    )

    # -----------------------------------------------------
    # Previous Channel Drilldown
    # -----------------------------------------------------

    previous_channel_body = {
        "dateRanges": [
            previous_period
        ],

        "dimensions": [
            {
                "name": "sessionDefaultChannelGroup"
            }
        ],

        "metrics": [
            {
                "name": "sessions"
            },
            {
                "name": "totalUsers"
            }
        ],

        "orderBys": [
            {
                "metric": {
                    "metricName": "sessions"
                },
                "desc": True
            }
        ],

        "limit": build_limit(
            req.channelLimit
        )
    }

    previous_channel_response = call_ga4(
        previous_channel_body
    )

    previous_channels = extract_channel_rows(
        previous_channel_response
    )

    # -----------------------------------------------------
    # Channel Comparison
    # -----------------------------------------------------

    channel_comparison = build_channel_comparison(
        channels,
        previous_channels
    )

    # -----------------------------------------------------
    # Business Questions
    # -----------------------------------------------------

    business_questions = build_business_questions(
        kpis,
        channels
    )

    # -----------------------------------------------------
    # Insights
    # -----------------------------------------------------

    insights = []

    if (
        sessions_change is not None
        and sessions_change >= 5
    ):
        insights.append(
            "セッション数は前期間より増加しています。"
        )

    if (
        sessions_change is not None
        and sessions_change <= -5
    ):
        insights.append(
            "セッション数が前期間より減少しています。"
            "チャネル別の確認が必要です。"
        )

    if channels:
        top_channel = channels[0]

        insights.append(
            (
                "最大流入チャネルは"
                f"{top_channel['channel']}で、"
                f"{top_channel['sessions']:,}"
                "セッションです。"
            )
        )

    unassigned = next(
        (
            row
            for row in channels
            if row["channel"] == "Unassigned"
        ),
        None
    )

    if unassigned:
        total_channel_sessions = sum(
            row["sessions"]
            for row in channels
        )

        if total_channel_sessions > 0:
            unassigned_ratio = round(
                (
                    unassigned["sessions"]
                    / total_channel_sessions
                ) * 100,
                1
            )

            if unassigned_ratio >= 5:
                insights.append(
                    (
                        "Unassignedが"
                        f"{unassigned_ratio}%"
                        "を占めています。"
                        "UTMやチャネル分類を確認する価値があります。"
                    )
                )

    # -----------------------------------------------------
    # Channel Comparison Insights
    # -----------------------------------------------------

    biggest_declines = [
        row
        for row in channel_comparison
        if (
            row.get("sessionsChangePercent")
            is not None
            and row["sessionsChangePercent"] <= -5
        )
    ]

    biggest_declines.sort(
        key=lambda row: row[
            "sessionsChangePercent"
        ]
    )

    if biggest_declines:
        decline = biggest_declines[0]

        insights.append(
            (
                f"{decline['channel']}は"
                f"前期間比"
                f"{decline['sessionsChangePercent']}%"
                "で、主要な減少チャネルの一つです。"
            )
        )

    biggest_growth = [
        row
        for row in channel_comparison
        if (
            row.get("sessionsChangePercent")
            is not None
            and row["sessionsChangePercent"] >= 5
        )
    ]

    biggest_growth.sort(
        key=lambda row: row[
            "sessionsChangePercent"
        ],
        reverse=True
    )

    if biggest_growth:
        growth = biggest_growth[0]

        insights.append(
            (
                f"{growth['channel']}は"
                f"前期間比+"
                f"{growth['sessionsChangePercent']}%"
                "で伸びています。"
            )
        )

    # -----------------------------------------------------
    # Final Response
    # -----------------------------------------------------

    return {
        "dashboard": "NTEC Pulse Dashboard",

        "generatedAt": datetime.now().isoformat(),

        "period": {
            "current": current_date_ranges[0],
            "previous": previous_period
        },

        "kpis": kpis,

        "drilldown": {
            "channels": channels,
            "channelComparison": channel_comparison
        },

        "insights": insights,

        "businessQuestions": business_questions,

        "lineage": {
            "ga4": {
                "source": "Google Analytics Data API",

                "propertyId": GA4_PROPERTY_ID,

                "dimensions": [
                    "sessionDefaultChannelGroup"
                ],

                "metrics": [
                    "sessions",
                    "totalUsers",
                    "screenPageViews",
                    "engagedSessions"
                ]
            },

            "bigQuery": {
                "source": "GA4 BigQuery Export",

                "project": BIGQUERY_PROJECT_ID,

                "dataset": BIGQUERY_DATASET,

                "availableDrilldowns": [
                    "/api/bq/page/pre-pages",
                    "/api/bq/conversion/pre-pages",
                    "/api/bq/user/path"
                ]
            }
        }
    }
