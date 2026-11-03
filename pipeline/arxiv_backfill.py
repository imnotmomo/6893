"""Collect paginated arXiv candidates while preserving current metadata provenance.

Run arXiv collection in one process across the project. The API's three-second,
single-connection limit applies across machines, not independently to each VM.
Search aliases produce candidates requiring review, not confirmed tool mentions.
"""

import datetime
import email.utils
from http.client import HTTPException
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


API_URL = "https://export.arxiv.org/api/query"
MAX_API_RESULTS = 30000
MAX_PAGE_SIZE = 100
MAX_BODY_BYTES = 2 * 1024 * 1024
TIMEOUT_SECONDS = 60
MAX_ATTEMPTS = 2
MIN_REQUEST_INTERVAL_SECONDS = 10.0
MAX_RETRY_WAIT_SECONDS = 30
ATOM = "{http://www.w3.org/2005/Atom}"
OPEN_SEARCH = "{http://a9.com/-/spec/opensearch/1.1/}"
UTC = datetime.timezone.utc
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_STARTED = None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


class _FeedError(ValueError):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


_OPENER = urllib.request.build_opener(_NoRedirect())


def _now():
    return datetime.datetime.now(UTC).isoformat()


def _failure(result, code, message, retryable=False):
    result["ok"] = False
    result["error"] = {"code": code, "message": message, "retryable": retryable}
    return result


def _midnight(value, name):
    if isinstance(value, str):
        try:
            value = datetime.date.fromisoformat(value)
        except ValueError:
            value = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            raise ValueError(f"{name} datetime must be timezone-aware.")
        value = value.astimezone(UTC)
        if value.time() != datetime.time():
            raise ValueError(f"{name} must be midnight UTC.")
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time(), tzinfo=UTC)
    raise ValueError(f"{name} must be an ISO date, date, or UTC-midnight datetime.")


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
                "User-Agent": "6893-AI-Technology-Radar-backfill/0.1",
                "Accept": "application/atom+xml",
                "Accept-Encoding": "identity",
            },
        )
        try:
            response = _OPENER.open(request, timeout=TIMEOUT_SECONDS)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            http = {
                "status": response.getcode(),
                "reason": str(response.reason),
                "response_url": response.geturl(),
                "headers": dict(response.headers.items()),
            }
            raw = response.read(MAX_BODY_BYTES + 1)
            http["body_truncated"] = len(raw) > MAX_BODY_BYTES
            return raw[:MAX_BODY_BYTES], http


def _text(element, name):
    return " ".join((element.findtext(ATOM + name) or "").split())


def _parse_page(raw, query, date_start, date_end, cursor, page_size):
    upper = raw.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise _FeedError("invalid_atom_response", "Unsupported XML declaration.")
    feed = ET.fromstring(raw)
    if feed.tag != ATOM + "feed":
        raise _FeedError("invalid_atom_response", "Response is not an Atom feed.")
    entries = feed.findall(ATOM + "entry")
    for entry in entries:
        if _text(entry, "title").casefold() == "error" or "/api/errors" in _text(entry, "id"):
            raise _FeedError("arxiv_api_error", _text(entry, "summary"))
    try:
        total = int(feed.findtext(OPEN_SEARCH + "totalResults"))
        returned_start = int(feed.findtext(OPEN_SEARCH + "startIndex"))
    except (TypeError, ValueError) as error:
        raise _FeedError("invalid_pagination", "Missing or invalid OpenSearch totals/index.") from error
    if total < 0 or returned_start != cursor or len(entries) > page_size:
        raise _FeedError("invalid_pagination", "Response totals/index do not match the requested page.")
    papers = []
    for entry in entries:
        identifier = _text(entry, "id")
        title = _text(entry, "title")
        published = _text(entry, "published")
        updated = _text(entry, "updated")
        if not all([identifier, title, published, updated]):
            raise _FeedError("invalid_atom_response", "A paper is missing required metadata.")
        try:
            submitted = datetime.datetime.fromisoformat(published.replace("Z", "+00:00"))
            if submitted.tzinfo is None:
                raise ValueError("missing timezone")
            submitted = submitted.astimezone(UTC)
        except ValueError as error:
            raise _FeedError("invalid_atom_response", "A paper has an invalid published timestamp.") from error
        if not date_start <= submitted < date_end:
            raise _FeedError("out_of_window_response", "A paper falls outside the requested UTC date window.")
        links = [dict(link.attrib) for link in entry.findall(ATOM + "link")]
        papers.append(
            {
                "id": identifier,
                "title": title,
                "published": published,
                "updated": updated,
                "authors": [_text(author, "name") for author in entry.findall(ATOM + "author")],
                "summary": _text(entry, "summary"),
                "link": next(
                    (link.get("href") for link in links if link.get("rel") == "alternate"),
                    identifier,
                ),
                "links": links,
                "candidate_query": query,
                "match_status": "candidate_unreviewed",
                "requires_manual_review": True,
                "historical_feature_eligible": False,
                "metadata_snapshot_semantics": "latest_metadata_collected_now",
            }
        )
    if len({paper["id"] for paper in papers}) != len(papers):
        raise _FeedError("invalid_pagination", "The page contains duplicate paper identifiers.", True)
    return papers, total


def _retry_wait(headers):
    value = next((value for key, value in headers.items() if key.lower() == "retry-after"), None)
    if value is None:
        return MIN_REQUEST_INTERVAL_SECONDS
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            seconds = (when - datetime.datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return MIN_REQUEST_INTERVAL_SECONDS
    return max(MIN_REQUEST_INTERVAL_SECONDS, seconds)


def fetch_arxiv_page(query, start, end_exclusive, cursor=0, page_size=100):
    """Return one dated candidate page; persist it before advancing next_cursor.

    Bounds are UTC dates, start inclusive and end exclusive. Pages sort by first
    submission date ascending. A complete page sequence covers the current API
    query result set, not historical metadata snapshots or confirmed mentions.
    Windows over the API's 30,000-result limit return error.code=split_required;
    callers must split the date window and restart each child window at zero.
    """
    result = {
        "ok": False,
        "source": "arxiv",
        "query": query if isinstance(query, str) else None,
        "submitted_date_query": None,
        "request_url": None,
        "raw_bytes": b"",
        "http": None,
        "error": None,
        "papers": [],
        "retrieved_at": None,
        "attempt_count": 0,
        "attempts": [],
        "requires_manual_review": True,
        "historical_feature_eligible": False,
        "metadata_snapshot_semantics": "latest_metadata_collected_now",
        "pagination": {
            "start": cursor if type(cursor) is int else None,
            "returned": 0,
            "total_results": None,
            "next_cursor": None,
            "complete": False,
            "split_required": False,
            "page_size": page_size if type(page_size) is int else None,
        },
    }
    if not isinstance(query, str) or not query.strip() or len(query) > 2000:
        return _failure(result, "invalid_query", "query must contain 1–2000 characters.")
    if type(cursor) is not int or not 0 <= cursor < MAX_API_RESULTS:
        return _failure(result, "invalid_cursor", "cursor must be an integer from 0 to 29999.")
    if type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE:
        return _failure(result, "invalid_page_size", "page_size must be an integer from 1 to 100.")
    try:
        date_start = _midnight(start, "start")
        date_end = _midnight(end_exclusive, "end_exclusive")
        if date_start >= date_end:
            raise ValueError("start must be earlier than end_exclusive.")
    except (TypeError, ValueError, OverflowError) as error:
        return _failure(result, "invalid_date_window", str(error))
    query = query.strip()
    result["query"] = query
    request_page_size = min(page_size, MAX_API_RESULTS - cursor)
    result["pagination"]["page_size"] = request_page_size
    result["date_window"] = {
        "start": date_start.isoformat(),
        "end_exclusive": date_end.isoformat(),
    }
    last_minute = date_end - datetime.timedelta(minutes=1)
    submitted_query = (
        f"({query}) AND submittedDate:[{date_start:%Y%m%d%H%M} TO {last_minute:%Y%m%d%H%M}]"
    )
    result["submitted_date_query"] = submitted_query
    result["request_url"] = API_URL + "?" + urllib.parse.urlencode(
        {
            "search_query": submitted_query,
            "start": cursor,
            "max_results": request_page_size,
            "sortBy": "submittedDate",
            "sortOrder": "ascending",
        }
    )
    for attempt in range(1, MAX_ATTEMPTS + 1):
        result["attempt_count"] = attempt
        try:
            raw, http = _request_once(result["request_url"])
            result.update(raw_bytes=raw, http=http, retrieved_at=_now())
            result["attempts"].append({"attempt": attempt, "retrieved_at": result["retrieved_at"], **http})
            if http["body_truncated"]:
                return _failure(result, "response_too_large", "Response exceeded the 2 MiB body limit.")
            if http["status"] != 200:
                retryable = http["status"] in {408, 429, 500, 502, 503, 504}
                _failure(result, "http_error", f"arXiv returned HTTP {http['status']}: {http['reason']}.", retryable)
                wait = _retry_wait(http["headers"])
                result["error"]["retry_after_seconds"] = wait
                if http["status"] == 429:
                    # Let the dispatcher persist one global cooldown instead of
                    # retrying another request within this already-limited call.
                    return result
                if retryable and attempt < MAX_ATTEMPTS and wait <= MAX_RETRY_WAIT_SECONDS:
                    time.sleep(wait)
                    continue
                return result
            try:
                papers, total = _parse_page(raw, query, date_start, date_end, cursor, request_page_size)
            except ET.ParseError as error:
                return _failure(result, "invalid_atom_response", str(error))
            except _FeedError as error:
                _failure(result, error.code, str(error), error.retryable)
                if error.retryable and attempt < MAX_ATTEMPTS:
                    continue
                return result
            result["papers"] = papers
            result["pagination"].update(returned=len(papers), total_results=total)
            if total > MAX_API_RESULTS:
                result["pagination"]["split_required"] = True
                return _failure(
                    result, "split_required",
                    f"Query has {total} results; split the UTC date window below the 30000-result API limit.",
                )
            if cursor > total or cursor + len(papers) > total:
                return _failure(result, "invalid_pagination", "Page extends beyond the reported result total.")
            if not papers and cursor < total:
                _failure(result, "unexpected_empty_page", "Empty page before the reported result total.", True)
                if attempt < MAX_ATTEMPTS:
                    continue
                return result
            next_cursor = cursor + len(papers)
            complete = next_cursor >= total
            result["pagination"].update(
                next_cursor=None if complete else next_cursor,
                complete=complete,
            )
            result.update(ok=True, error=None)
            return result
        except (urllib.error.URLError, TimeoutError, OSError, HTTPException) as error:
            result["retrieved_at"] = _now()
            result["attempts"].append({"attempt": attempt, "retrieved_at": result["retrieved_at"], "error": f"{type(error).__name__}: {error}"})
            _failure(result, "network_error", f"{type(error).__name__}: {error}", True)
            if attempt == MAX_ATTEMPTS:
                return result
    return result
