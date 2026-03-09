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


# -----------------------------
# Google OAuth Token Handling
# -----------------------------

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


# -----------------------------
# GA4 API Call
# -----------------------------

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


# -----------------------------
# Utility
# -----------------------------

def build_date_ranges(start_date: Optional[str], end_date: Optional[str], days: int = 30):
    if start_date and end_date:
        return [{"startDate": start_date, "endDate": end_date}]
    return [{"startDate": f"{days}daysAgo", "endDate": "today"}]


def get_display_dimension(display_dimension: str):
    if display_dimension == "pagePath":
        return "pagePath"
    return "pageTitle"


def get_match_field(match_type: str):
    if match_type == "title":
        return "pageTitle"
    if match_type == "path":
        return "pagePath"
    return "pageLocation"


# -----------------------------
# Health
# -----------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------------
# Channel Report
# -----------------------------

@app.post("/api/ga4/standard/channel")
def channel_report():

    body = {
        "dateRanges": [{"startDate": "30daysAgo", "endDate": "today"}],
        "dimensions": [{"name": "sessionDefaultChannelGroup"}],
        "metrics": [{"name": "sessions"}, {"name": "totalUsers"}]
    }

    return call_ga4(body)


# -----------------------------
# Page Flow
# -----------------------------

@app.post("/api/ga4/page/flow")
def page_flow():

    body = {
        "dateRanges": [{"startDate": "30daysAgo", "endDate": "today"}],
        "dimensions": [{"name": "pageReferrer"}, {"name": "pagePath"}],
        "metrics": [{"name": "screenPageViews"}]
    }

    return call_ga4(body)


# -----------------------------
# Conversion Pages
# -----------------------------

@app.post("/api/ga4/conversion/pages")
def conversion_pages():

    body = {
        "dateRanges": [{"startDate": "30daysAgo", "endDate": "today"}],
        "dimensions": [{"name": "pagePath"}],
        "metrics": [{"name": "sessions"}],
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


# -----------------------------
# Conversion Path
# -----------------------------

@app.post("/api/ga4/conversion/path")
def conversion_path():

    body = {
        "dateRanges": [{"startDate": "30daysAgo", "endDate": "today"}],
        "dimensions": [{"name": "landingPage"}, {"name": "pagePath"}],
        "metrics": [{"name": "sessions"}],
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


# -----------------------------
# Conversion Summary
# -----------------------------

class ConversionSummaryRequest(BaseModel):
    days: int = 30
    eventName: str = "generate_lead"


@app.post("/api/ga4/conversion/summary")
def conversion_summary(req: ConversionSummaryRequest):

    body = {
        "dateRanges": [{
            "startDate": f"{req.days}daysAgo",
            "endDate": "today"
        }],
        "dimensions": [{"name": "eventName"}],
        "metrics": [{"name": "eventCount"}],
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


# -----------------------------
# Thanks Page Summary
# -----------------------------

class ThanksPageSummaryRequest(BaseModel):
    days: int = 30
    thanksPage: str = "/contact/thanks/"


@app.post("/api/ga4/conversion/thanks-summary")
def thanks_summary(req: ThanksPageSummaryRequest):

    body = {
        "dateRanges": [{
            "startDate": f"{req.days}daysAgo",
            "endDate": "today"
        }],
        "dimensions": [{"name": "pagePath"}],
        "metrics": [{"name": "screenPageViews"}, {"name": "sessions"}],
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


# -----------------------------
# Page Flow From Page
# -----------------------------

class PageFlowFromPageRequest(BaseModel):
    sourcePage: str
    days: int = 30
    limit: int = 20


@app.post("/api/ga4/page/flow/from-page")
def page_flow_from_page(req: PageFlowFromPageRequest):

    body = {
        "dateRanges": [{
            "startDate": f"{req.days}daysAgo",
            "endDate": "today"
        }],
        "dimensions": [{"name": "pageReferrer"}, {"name": "pagePath"}],
        "metrics": [{"name": "screenPageViews"}],
        "dimensionFilter": {
            "filter": {
                "fieldName": "pageReferrer",
                "stringFilter": {
                    "matchType": "CONTAINS",
                    "value": req.sourcePage
                }
            }
        },
        "orderBys": [{
            "metric": {"metricName": "screenPageViews"},
            "desc": True
        }],
        "limit": str(req.limit)
    }

    return call_ga4(body)


# -----------------------------
# Exit Pages
# -----------------------------

class ExitPagesRequest(BaseModel):
    days: int = 30
    limit: int = 20


@app.post("/api/ga4/page/exits")
def page_exits(req: ExitPagesRequest):

    body = {
        "dateRanges": [{
            "startDate": f"{req.days}daysAgo",
            "endDate": "today"
        }],
        "dimensions": [{"name": "pagePath"}],
        "metrics": [
            {"name": "sessions"},
            {"name": "screenPageViews"},
            {"name": "bounceRate"}
        ],
        "orderBys": [{
            "metric": {"metricName": "bounceRate"},
            "desc": True
        }],
        "limit": str(req.limit)
    }

    return call_ga4(body)


# -----------------------------
# Previous Page Before Target
# -----------------------------

class PreviousPageRequest(BaseModel):
    targetPage: str
    matchType: str = "url"
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    days: int = 30
    displayDimension: str = "pageTitle"
    limit: int = 20


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

        "dimensionFilter": {
            "filter": {
                "fieldName": match_field,
                "stringFilter": {
                    "matchType": "CONTAINS",
                    "value": req.targetPage
                }
            }
        },

        "orderBys": [{
            "metric": {"metricName": "screenPageViews"},
            "desc": True
        }],

        "limit": str(req.limit)
    }

    return call_ga4(body)
