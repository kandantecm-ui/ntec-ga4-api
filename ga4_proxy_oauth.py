from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import json
import os

app = FastAPI()

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
TOKEN_FILE = "token.json"


def load_token():
    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_token(data):
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)


def refresh_access_token():
    token_data = load_token()

    refresh_token = token_data["refresh_token"]
    client_id = token_data["client_id"]
    client_secret = token_data["client_secret"]

    url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    r = requests.post(url, data=payload)

    if r.status_code != 200:
        raise Exception(f"Token refresh failed: {r.text}")

    new_token = r.json()["access_token"]
    token_data["token"] = new_token
    save_token(token_data)

    return new_token


def get_access_token():
    token_data = load_token()
    return token_data["token"]


def call_ga4(data):
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


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/ga4/standard/channel")
def channel_report():
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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
    if not GA4_PROPERTY_ID:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")

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