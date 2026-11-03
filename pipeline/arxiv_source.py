"""Fetch one small page of unreviewed arXiv candidates using the standard library.

This is a current metadata pilot, not a historical mention count. Use one collector
process across the project: arXiv's request limit applies across machines too.
"""

import datetime
import email.utils
import http.client
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


API_URL = "https://export.arxiv.org/api/query"
MAX_BODY_BYTES = 2 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 15
MIN_REQUEST_INTERVAL_SECONDS = 3.0
MAX_ATTEMPTS = 2
MAX_RETRY_WAIT_SECONDS = 30
MAX_PILOT_RESULTS = 100
ATOM = "{http://www.w3.org/2005/Atom}"
OPEN_SEARCH = "{http://a9.com/-/spec/opensearch/1.1/}"
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_STARTED = None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect())


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _failure(result, code, message, retryable=False):
    result["ok"] = False
    result["error"] = {
        "code": code,
        "message": message,
        "retryable": retryable,
    }
    return result


def _request_once(url):
    global _LAST_REQUEST_STARTED
    with _REQUEST_LOCK:
        if _LAST_REQUEST_STARTED is not None:
            remaining = (
                _LAST_REQUEST_STARTED + MIN_REQUEST_INTERVAL_SECONDS - time.monotonic()
            )
            if remaining > 0:
                time.sleep(remaining)
        _LAST_REQUEST_STARTED = time.monotonic()
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "6893-AI-Technology-Radar-pilot/0.1",
                "Accept": "application/atom+xml",
                "Accept-Encoding": "identity",
            },
        )
        try:
            response = _OPENER.open(request, timeout=REQUEST_TIMEOUT_SECONDS)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            metadata = {
                "status": response.getcode(),
                "reason": str(response.reason),
                "response_url": response.geturl(),
                "headers": dict(response.headers.items()),
            }
            raw = response.read(MAX_BODY_BYTES + 1)
            metadata["body_truncated"] = len(raw) > MAX_BODY_BYTES
            return raw[:MAX_BODY_BYTES], metadata


def _text(element, name):
    return " ".join((element.findtext(ATOM + name) or "").split())


def _parse_candidates(raw, query, max_results):
    if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise ValueError("The response contains an unsupported XML declaration.")
    feed = ET.fromstring(raw)
    if feed.tag != ATOM + "feed":
        raise ValueError("The response is not an Atom feed.")
    entries = feed.findall(ATOM + "entry")
    if len(entries) > max_results:
        raise ValueError("The response has more entries than the requested pilot page.")
    papers = []
    for entry in entries:
        identifier = _text(entry, "id")
        title = _text(entry, "title")
        if title.casefold() == "error" or "/api/errors" in identifier:
            raise ValueError("arXiv API error: " + _text(entry, "summary"))
        published = _text(entry, "published")
        updated = _text(entry, "updated")
        if not all([identifier, title, published, updated]):
            raise ValueError("An Atom entry is missing a required paper field.")
        links = [dict(link.attrib) for link in entry.findall(ATOM + "link")]
        primary_link = next(
            (link.get("href") for link in links if link.get("rel") == "alternate"),
            identifier,
        )
        papers.append(
            {
                "id": identifier,
                "title": title,
                "published": published,
                "updated": updated,
                "authors": [
                    _text(author, "name") for author in entry.findall(ATOM + "author")
                ],
                "summary": _text(entry, "summary"),
                "link": primary_link,
                "links": links,
                "candidate_query": query,
                "match_status": "candidate_unreviewed",
                "requires_manual_review": True,
                "historical_feature_eligible": False,
            }
        )
    total = feed.findtext(OPEN_SEARCH + "totalResults")
    total = int(total) if total is not None else None
    return papers, total


def _retry_after_seconds(headers):
    value = next(
        (value for key, value in headers.items() if key.lower() == "retry-after"),
        None,
    )
    if value is None:
        return MIN_REQUEST_INTERVAL_SECONDS
    try:
        return max(MIN_REQUEST_INTERVAL_SECONDS, float(value))
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=datetime.timezone.utc)
            seconds = (
                when - datetime.datetime.now(datetime.timezone.utc)
            ).total_seconds()
            return max(MIN_REQUEST_INTERVAL_SECONDS, seconds)
        except (TypeError, ValueError, OverflowError):
            return MIN_REQUEST_INTERVAL_SECONDS


def fetch_candidate_arxiv_metadata(query, max_results=5):
    """Return raw bytes, HTTP provenance, candidate papers, or a structured error.

    Fetches start=0 only, with a project cap of 100 results (default 5). Requests
    are serialized and paced at least three seconds apart, including retries.
    There are at most two HTTP attempts; long Retry-After delays are reported
    instead of retried. Callers should persist/reuse the raw response.
    """
    result = {
        "ok": False,
        "source": "arxiv",
        "query": query if isinstance(query, str) else None,
        "request_url": None,
        "raw_bytes": b"",
        "http": None,
        "retrieved_at": None,
        "attempt_count": 0,
        "attempts": [],
        "papers": [],
        "error": None,
        "requires_manual_review": True,
        "historical_feature_eligible": False,
        "pagination": {
            "start": 0,
            "max_results": max_results if type(max_results) is int else None,
            "returned_candidates": 0,
            "total_results": None,
            "complete_history": False,
        },
    }
    if not isinstance(query, str) or not query.strip() or len(query) > 2000:
        return _failure(result, "invalid_query", "query must contain 1–2000 characters.")
    if type(max_results) is not int or not 1 <= max_results <= MAX_PILOT_RESULTS:
        return _failure(
            result, "invalid_max_results", "The pilot accepts max_results from 1 to 100."
        )
    query = query.strip()
    result["query"] = query
    result["request_url"] = API_URL + "?" + urllib.parse.urlencode(
        {
            "search_query": query,
            "start": 0,
            "max_results": max_results,
            "sortBy": "relevance",
            "sortOrder": "descending",
        }
    )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        result["attempt_count"] = attempt
        try:
            raw, metadata = _request_once(result["request_url"])
            result["raw_bytes"] = raw
            result["http"] = metadata
            result["retrieved_at"] = _utc_now()
            result["attempts"].append(
                {"attempt": attempt, "retrieved_at": result["retrieved_at"], **metadata}
            )
            if metadata["body_truncated"]:
                return _failure(
                    result, "response_too_large", "Response exceeded the 2 MiB body limit."
                )
            status = metadata["status"]
            if status != 200:
                retryable = status in {408, 429, 500, 502, 503, 504}
                _failure(
                    result,
                    "http_error",
                    f"arXiv returned HTTP {status}: {metadata['reason']}.",
                    retryable,
                )
                retry_wait = _retry_after_seconds(metadata["headers"])
                result["error"]["retry_after_seconds"] = retry_wait
                if retryable and attempt < MAX_ATTEMPTS:
                    if retry_wait <= MAX_RETRY_WAIT_SECONDS:
                        time.sleep(retry_wait)
                        continue
                return result
            try:
                papers, total = _parse_candidates(raw, query, max_results)
            except (ET.ParseError, ValueError) as error:
                return _failure(result, "invalid_atom_response", str(error))
            result["papers"] = papers
            result["pagination"].update(
                returned_candidates=len(papers), total_results=total
            )
            result["ok"] = True
            result["error"] = None
            return result
        except (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            http.client.HTTPException,
        ) as error:
            result["retrieved_at"] = _utc_now()
            result["attempts"].append(
                {
                    "attempt": attempt,
                    "retrieved_at": result["retrieved_at"],
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            _failure(result, "network_error", f"{type(error).__name__}: {error}", True)
            if attempt == MAX_ATTEMPTS:
                return result
    return result
