from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests
import os

app = FastAPI()

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
GOOGLE_ACCESS_TOKEN = os.getenv("GOOGLE_ACCESS_TOKEN")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")


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

class DateRangeRequest(BaseModel):
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    limit: int = 20


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
    matchType: str = "path"          # path / url / title
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    displayDimension: str = "pageTitle"
    limit: int = 20


class PreviousPageRequest(BaseModel):
    targetPage: str
    matchType: str = "url"           # url / path / title
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
# Health
# =============================

@app.get("/health")
def health():
    return {"status": "ok"}


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
    source_field = get_match_field(req.matchType)
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

    result = call_ga4(body)
    result["requestInfo"] = {
        "sourcePage": req.sourcePage,
        "matchType": req.matchType,
        "displayDimension": display_dimension,
        "note": "pageReferrer ベースのため、厳密な内部遷移ではなく参照元URL集計です"
    }
    return result


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

    result = call_ga4(body)
    result["requestInfo"] = {
        "targetPage": req.targetPage,
        "matchType": req.matchType,
        "displayDimension": display_dimension,
        "note": "pageReferrer ベースのため、イベントページ直前の厳密なユーザー単位遷移ではなく参照元URL集計です"
    }
    return result


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

    result = call_ga4(body)
    result["requestInfo"] = {
        "eventName": req.eventName,
        "displayDimension": display_dimension,
        "note": "landingPage × page の集計であり、厳密な多段階パス分析ではありません"
    }
    return result


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
