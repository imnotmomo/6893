"""Bounded BigQuery collectors. No network access occurs when importing this module.

Every new billable job is preceded by a free dry run and an explicit bytes-cap
check. A pending result must be resumed, never interpreted as zero activity.
google-auth[requests] is optional when an in-memory bearer token is supplied.

Schemas: https://blog.pypi.org/posts/2026-08-31-download-counts/
https://github.com/igrigorik/gharchive.org/blob/master/bigquery/schema.js
REST: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
"""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request


API = "https://bigquery.googleapis.com/bigquery/v2"
USER_AGENT = "EECS6893-AITechnologyRadar/0.2"
SCOPES = ["https://www.googleapis.com/auth/bigquery"]
MAX_WINDOW_DAYS = 7
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_LEDGER_BYTES = 32 * 1024 * 1024
MAX_RESULT_ROWS = 100000


class SourceError(Exception):
    def __init__(self, status, kind, message, http_status=None):
        super().__init__(message)
        self.status = status
        self.kind = kind
        self.http_status = http_status


def _encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _date(value):
    if isinstance(value, datetime):
        raise ValueError("Use an explicit UTC date, not a datetime")
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _dates(start, end_exclusive):
    start, end = _date(start), _date(end_exclusive)
    days = (end - start).days
    if not 1 <= days <= MAX_WINDOW_DAYS:
        raise ValueError("Each request must cover one to seven days")
    if end > datetime.now(timezone.utc).date():
        raise ValueError("End must exclude today and future UTC dates")
    return start, end


def _list(value):
    if value is None:
        return []
    return value if isinstance(value, (list, tuple, set)) else [value]


def _tools(value, source):
    responses = value.get("responses", []) if isinstance(value, dict) else []
    records = {
        row["tool_id"]: row.get("record", {})
        for row in responses
        if row.get("source") == "github" and row.get("tool_id")
    }
    value = value.get("tools", []) if isinstance(value, dict) else value
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError("tools must be a nonempty list or a manifest with tools")
    output, seen = [], set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Each tool must be a mapping")
        tool_id = str(item.get("tool_id") or "").strip()
        if not tool_id or tool_id in seen:
            raise ValueError("tool_id must be nonempty and unique")
        seen.add(tool_id)
        package = re.sub(r"[-_.]+", "-", str(item.get("pypi_package") or "").lower())
        record = records.get(tool_id, {})
        github = item.get("github") if isinstance(item.get("github"), dict) else {}
        identifiers = []
        for key in ("github_repo_ids", "github_repo_id", "github_id", "repo_id"):
            identifiers.extend(_list(item.get(key)))
        identifiers.extend(_list(github.get("id") or record.get("id")))
        ids = []
        for identifier in identifiers:
            if isinstance(identifier, bool) or not re.fullmatch(r"[1-9][0-9]*", str(identifier)):
                raise ValueError("GitHub repository IDs must be positive integers")
            ids.append(str(int(identifier)))
        aliases = _list(item.get("github_repo_aliases")) + _list(item.get("github_aliases"))
        names = aliases + [item.get("github_repo"), github.get("full_name"), record.get("full_name")]
        names = sorted({str(name).strip().lower() for name in names if name})
        if source == "pypi" and not re.fullmatch(r"[a-z0-9][a-z0-9-]*", package):
            raise ValueError("Every PyPI tool needs a valid pypi_package")
        if source == "gharchive" and not (ids or names):
            raise ValueError("Every GH Archive tool needs a repository ID or repository name")
        if any(not re.fullmatch(r"[^/\s]+/[^/\s]+", name) for name in names):
            raise ValueError("GitHub repository names must use owner/repository")
        output.append({"tool_id": tool_id, "package": package, "repo_ids": sorted(set(ids)), "repo_names": names})
    return sorted(output, key=lambda item: item["tool_id"])


def _parameter(name, kind, value):
    return {"name": name, "parameterType": {"type": kind}, "parameterValue": {"value": str(value)}}


def _array_parameter(name, kind, values):
    return {
        "name": name,
        "parameterType": {"type": "ARRAY", "arrayType": {"type": kind}},
        "parameterValue": {"arrayValues": [{"value": str(value)} for value in values]},
    }


def build_query(source, tools, start, end_exclusive):
    """Build GoogleSQL and typed REST parameters without authentication or queries."""
    aliases = {"pypi_downloads": "pypi", "githubarchive": "gharchive", "github": "gharchive"}
    source = aliases.get(source, source)
    if source not in ("pypi", "gharchive"):
        raise ValueError("source must be pypi or gharchive")
    start, end = _dates(start, end_exclusive)
    mappings = _tools(tools, source)
    parameters = [
        _parameter("start_date", "DATE", start.isoformat()),
        _parameter("end_date", "DATE", end.isoformat()),
        _parameter("tool_map", "STRING", _encode(mappings)),
    ]
    if source == "pypi":
        parameters.append(_array_parameter("packages", "STRING", sorted({item["package"] for item in mappings})))
        sql = r"""WITH tool_map AS (
  SELECT JSON_VALUE(item, '$.tool_id') AS tool_id,
         JSON_VALUE(item, '$.package') AS package
  FROM UNNEST(JSON_QUERY_ARRAY(@tool_map)) AS item
), daily AS (
  SELECT project AS package, DATE(timestamp, 'UTC') AS date,
         COUNT(*) AS logged_objects,
         COUNTIF(REGEXP_CONTAINS(file.filename, r'\.(whl|tar\.gz|zip)$')) AS downloads
  FROM `bigquery-public-data.pypi.file_downloads`
  WHERE timestamp >= TIMESTAMP(@start_date, 'UTC')
    AND timestamp < TIMESTAMP(@end_date, 'UTC')
    AND project IN UNNEST(@packages)
  GROUP BY package, date
)
SELECT tool_map.tool_id, daily.package, daily.date, daily.downloads,
       daily.logged_objects, daily.logged_objects - daily.downloads AS excluded_requests
FROM daily JOIN tool_map USING (package)
ORDER BY tool_id, date"""
    else:
        ids = sorted({identifier for item in mappings for identifier in item["repo_ids"]}, key=int)
        names = sorted({name for item in mappings if not item["repo_ids"] for name in item["repo_names"]})
        parameters += [_array_parameter("repository_ids", "INT64", ids), _array_parameter("repository_names", "STRING", names)]
        month_days = {}
        for offset in range((end - start).days):
            day = start + timedelta(days=offset)
            month_days.setdefault(day.strftime("%Y%m"), []).append(day.strftime("%d"))
        scans = []
        repo_id_projection = "repo.id" if ids else "CAST(NULL AS INT64)"
        repo_name_projection = "LOWER(repo.name)" if names else "CAST(NULL AS STRING)"
        filters = (["repo.id IN UNNEST(@repository_ids)"] if ids else [])
        if names:
            filters.append("LOWER(repo.name) IN UNNEST(@repository_names)")
        repository_filter = " OR ".join(filters)
        for month, days in sorted(month_days.items()):
            scans.append(f"""SELECT id, created_at, type, actor.id AS actor_id,
         {repo_id_projection} AS repo_id, {repo_name_projection} AS repo_name
  FROM `githubarchive.day.{month}*`
  WHERE _TABLE_SUFFIX BETWEEN '{min(days)}' AND '{max(days)}'
    AND created_at >= TIMESTAMP(@start_date, 'UTC')
    AND created_at < TIMESTAMP(@end_date, 'UTC')
    AND ({repository_filter})""")
        sql = """WITH tool_map AS (
  SELECT JSON_VALUE(item, '$.tool_id') AS tool_id,
         ARRAY(SELECT CAST(identifier AS INT64)
               FROM UNNEST(JSON_VALUE_ARRAY(item, '$.repo_ids')) AS identifier) AS repo_ids,
         JSON_VALUE_ARRAY(item, '$.repo_names') AS repo_names
  FROM UNNEST(JSON_QUERY_ARRAY(@tool_map)) AS item
), events AS (
  """ + "\n  UNION ALL\n  ".join(scans) + """
), matched AS (
  SELECT tool_map.tool_id, events.*
  FROM events JOIN tool_map
    ON events.repo_id IN UNNEST(tool_map.repo_ids)
       OR (ARRAY_LENGTH(tool_map.repo_ids) = 0
           AND events.repo_name IN UNNEST(tool_map.repo_names))
), deduplicated AS (
  SELECT * FROM matched
  QUALIFY id IS NULL OR id = ''
    OR ROW_NUMBER() OVER (
      PARTITION BY tool_id, id ORDER BY created_at DESC, type
    ) = 1
), daily AS (
  SELECT tool_id, DATE(created_at, 'UTC') AS date, type AS event_type,
         '' AS action,
         COUNTIF(id IS NOT NULL AND id != '') AS event_count,
         COUNT(DISTINCT IF(id IS NOT NULL AND id != '', actor_id, NULL)) AS active_actors,
         CAST(NULL AS INT64) AS merged_pr_events,
         COUNTIF(id IS NULL OR id = '') AS missing_event_id_rows
  FROM deduplicated
  GROUP BY tool_id, date, event_type
), window_actors AS (
  SELECT tool_id,
         COUNT(DISTINCT IF(id IS NOT NULL AND id != '', actor_id, NULL)) AS active_actors,
         COUNTIF(id IS NULL OR id = '') AS missing_event_id_rows
  FROM deduplicated
  GROUP BY tool_id
)
SELECT * FROM daily
UNION ALL
SELECT tool_map.tool_id, @start_date AS date, '__all_types__' AS event_type,
       '__window_unique_actors__' AS action, CAST(NULL AS INT64) AS event_count,
       COALESCE(window_actors.active_actors, 0) AS active_actors,
       CAST(NULL AS INT64) AS merged_pr_events,
       COALESCE(window_actors.missing_event_id_rows, 0) AS missing_event_id_rows
FROM tool_map LEFT JOIN window_actors USING (tool_id)
ORDER BY tool_id, date, event_type, action"""
    return {
        "source": source, "query_sql": sql, "query_parameters": parameters,
        "start": start.isoformat(), "end_exclusive": end.isoformat(), "tools": mappings,
    }


def _clean_error(message):
    message = re.sub(r"(?i)bearer\s+\S+", "Bearer [redacted]", str(message))
    message = re.sub(r"(?i)(access_token|refresh_token|client_secret)\s*[=:]\s*[^\s,}]+", r"\1=[redacted]", message)
    return message[:2000]


class _Client:
    def __init__(self, credentials):
        self.ledger = []
        self.ledger_size = 0
        self.token = None
        self.session = None
        if isinstance(credentials, str):
            if not credentials.strip() or any(char.isspace() for char in credentials):
                raise SourceError("blocked", "missing_auth", "A nonempty in-memory access token is required")
            self.token = credentials
        elif credentials is not None and callable(getattr(credentials, "request", None)):
            self.session = credentials
        else:
            try:
                import google.auth
                from google.auth.transport.requests import AuthorizedSession
                if credentials is None:
                    credentials, _ = google.auth.default(scopes=SCOPES)
                self.session = AuthorizedSession(credentials, refresh_timeout=20)
            except Exception as error:
                raise SourceError("blocked", "missing_auth", "Provide credentials or configure ADC with google-auth[requests]; " + type(error).__name__) from None

    def request(self, method, path, payload=None, params=None, allow_404=False, allow_409=False, timeout=20):
        url = API + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        deadline = time.monotonic() + float(timeout)
        for attempt in range(3):
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SourceError("error", "transport_timeout", "BigQuery request exceeded its bounded retry deadline")
                headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
                if self.token:
                    headers["Authorization"] = "Bearer " + self.token
                    data = None if payload is None else _encode(payload).encode()
                    if data is not None:
                        headers["Content-Type"] = "application/json"
                    request = urllib.request.Request(url, data=data, headers=headers, method=method)
                    try:
                        with urllib.request.urlopen(request, timeout=remaining) as response:
                            status, body = response.status, response.read(MAX_RESPONSE_BYTES + 1)
                    except urllib.error.HTTPError as error:
                        status, body = error.code, error.read(MAX_RESPONSE_BYTES + 1)
                else:
                    response = self.session.request(method, url, json=payload, headers=headers, timeout=remaining)
                    status, body = response.status_code, response.content
                if len(body) > MAX_RESPONSE_BYTES:
                    raise SourceError("error", "response_limit", "BigQuery response exceeds 8 MiB")
                try:
                    parsed = json.loads(body) if body else {}
                except ValueError:
                    if status < 400:
                        raise
                    parsed = {"error": {"message": _clean_error(body.decode("utf-8", errors="replace"))}}
                if not isinstance(parsed, dict):
                    raise ValueError("Expected a JSON object")
                self.ledger_size += len(body)
                if self.ledger_size > MAX_LEDGER_BYTES:
                    raise SourceError("error", "response_limit", "BigQuery response ledger exceeds 32 MiB")
                self.ledger.append({"method": method, "url": url, "http_status": status, "response": parsed})
                if status == 404 and allow_404:
                    return None
                if status == 409 and allow_409:
                    return None
                if 200 <= status < 300:
                    return parsed
                message = parsed.get("error", {}).get("message", "BigQuery request failed")
                if status in (429, 500, 502, 503, 504) and attempt < 2:
                    time.sleep(min(attempt + 1, max(0, deadline - time.monotonic())))
                    continue
                state = "blocked" if status in (401, 403, 404, 429) else "error"
                raise SourceError(state, "bigquery_http", _clean_error(message), status)
            except SourceError:
                raise
            except Exception as error:
                if type(error).__module__.startswith("google.auth"):
                    raise SourceError("blocked", "authentication", "Google credential refresh failed: " + type(error).__name__) from None
                if attempt < 2 and isinstance(error, (OSError, TimeoutError, urllib.error.URLError)):
                    time.sleep(min(attempt + 1, max(0, deadline - time.monotonic())))
                    continue
                raise SourceError("blocked" if "auth" in type(error).__name__.lower() else "error", "transport", type(error).__name__ + ": " + _clean_error(error)) from None


def _base(source, project, location):
    return {
        "ok": False, "status": "blocked", "source": source, "rows": [], "raw_bytes": b"",
        "query_sql": None, "job_id": None, "error": None,
        "request_metadata": {"project": project, "location": location, "raw_format": "bigquery_rest_response_ledger_json"},
        "coverage": {"query_completed": False, "missing_rows_are_zero": False},
    }


def _finish(result, client=None):
    if client is not None:
        result["raw_bytes"] = _encode({"responses": client.ledger}).encode()
    return result


def _failed(result, error, client=None):
    result.update(ok=False, status=error.status, rows=[])
    result["error"] = {"type": error.kind, "message": _clean_error(error), "http_status": error.http_status}
    result["coverage"].update(query_completed=False, missing_rows_are_zero=False)
    return _finish(result, client)


def _path(project, job_id=None):
    path = "/projects/" + urllib.parse.quote(project, safe="") + "/jobs"
    return path if job_id is None else path + "/" + urllib.parse.quote(job_id, safe="")


def _check_job_error(job):
    error = job.get("status", {}).get("errorResult")
    if error:
        reason = error.get("reason", "query_error")
        blocked = reason in ("accessDenied", "billingNotEnabled", "quotaExceeded", "rateLimitExceeded", "notFound", "resourcesExceeded")
        raise SourceError("blocked" if blocked else "error", reason, error.get("message", "BigQuery job failed"))


def _parameter_type(parameter_type):
    aliases = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL", "RECORD": "STRUCT"}
    kind = str(parameter_type["type"]).upper()
    kind = aliases.get(kind, kind)
    if kind == "ARRAY":
        return kind, _parameter_type(parameter_type["arrayType"])
    if kind == "STRUCT":
        return kind, tuple((field["name"], _parameter_type(field["type"]))
                           for field in parameter_type.get("structTypes", []))
    return kind,


def _parameter_value(parameter_type, value):
    kind = parameter_type[0]
    if kind == "ARRAY":
        # BigQuery omits an empty repeated arrayValues field in returned jobs.
        if "value" in value:
            if value["value"] is not None or "arrayValues" in value:
                raise ValueError("Invalid ARRAY query parameter value")
            return "null_array",
        entries = value.get("arrayValues", [])
        if not isinstance(entries, list):
            raise ValueError("Invalid ARRAY query parameter entries")
        return tuple(_parameter_value(parameter_type[1], entry) for entry in entries)
    if kind == "STRUCT":
        entries = value.get("structValues", {})
        if set(entries) - {name for name, _ in parameter_type[1]}:
            raise ValueError("Unexpected STRUCT query parameter fields")
        return tuple((name, _parameter_value(field_type, entries.get(name, {})))
                     for name, field_type in parameter_type[1])
    scalar = value.get("value")
    if scalar is None:
        return None
    if kind == "INT64":
        if isinstance(scalar, bool) or not re.fullmatch(r"[+-]?[0-9]+", str(scalar)):
            raise ValueError("Invalid integer query parameter")
        return int(scalar)
    if kind == "BOOL":
        if str(scalar).lower() not in ("true", "false"):
            raise ValueError("Invalid boolean query parameter")
        return str(scalar).lower() == "true"
    return str(scalar)


def _canonical_parameters(parameters):
    canonical = {}
    for parameter in parameters:
        name = parameter["name"]
        if name in canonical:
            raise ValueError("Duplicate named query parameter")
        parameter_type = _parameter_type(parameter["parameterType"])
        canonical[name] = (parameter_type, _parameter_value(parameter_type, parameter.get("parameterValue", {})))
    return canonical


def _check_query_match(job, result):
    metadata = result["request_metadata"]
    query = job.get("configuration", {}).get("query", {})
    reference = job.get("jobReference", {})
    try:
        matches = (
            query.get("query") == result["query_sql"]
            and int(query["maximumBytesBilled"]) == metadata["maximum_bytes_billed"]
            and _canonical_parameters(query.get("queryParameters", []))
                == _canonical_parameters(metadata["query_parameters"])
            and (not reference.get("projectId") or reference["projectId"] == metadata["project"])
            and (not reference.get("location") or reference["location"].upper() == metadata["location"].upper())
            and (not reference.get("jobId") or not result.get("job_id") or reference["jobId"] == result["job_id"])
            and query.get("useLegacySql", False) is False
        )
    except (ValueError, TypeError, KeyError, AttributeError):
        matches = False
    if not matches:
        raise SourceError("blocked", "job_id_conflict", "Existing job has different SQL, parameters, bytes cap, or project/location")


def _check_tables(client, plan):
    if plan["source"] != "gharchive":
        return []
    verified = []
    day, end = _date(plan["start"]), _date(plan["end_exclusive"])
    while day < end:
        table_id = day.strftime("%Y%m%d")
        metadata = client.request("GET", "/projects/githubarchive/datasets/day/tables/" + table_id,
                                  params={"fields": "tableReference,numRows,lastModifiedTime"})
        if int(metadata.get("numRows", "0")) <= 0:
            raise SourceError("blocked", "source_day_empty", "GH Archive day " + table_id + " has no confirmed rows; do not infer tool-level zeros")
        verified.append({"date": day.isoformat(), "num_rows": int(metadata["numRows"]), "last_modified_ms": metadata.get("lastModifiedTime")})
        day += timedelta(days=1)
    return verified


def _decode_rows(schema, rows):
    fields = schema.get("fields", [])
    output = []
    for row in rows:
        cells = row.get("f", [])
        if len(cells) != len(fields):
            raise SourceError("error", "invalid_result", "BigQuery row/schema width mismatch")
        record = {}
        for field, cell in zip(fields, cells):
            value = cell.get("v")
            if value is not None:
                if field["type"] in ("INTEGER", "INT64"):
                    value = int(value)
                elif field["type"] in ("FLOAT", "FLOAT64"):
                    value = float(value)
                elif field["type"] in ("BOOLEAN", "BOOL"):
                    value = value is True or str(value).lower() == "true"
            record[field["name"]] = value
        output.append(record)
    return output


def _poll(client, result, timeout_seconds):
    deadline = time.monotonic() + min(55, max(0, float(timeout_seconds)))
    metadata = result["request_metadata"]
    project, location, job_id = metadata["project"], metadata["location"], result["job_id"]
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            result.update(status="pending", ok=False, rows=[])
            metadata["resume_required"] = True
            return _finish(result, client)
        job = client.request("GET", _path(project, job_id), params={"location": location}, timeout=min(20, max(1, remaining)))
        _check_query_match(job, result)
        _check_job_error(job)
        metadata["job_state"] = job.get("status", {}).get("state")
        metadata["statistics"] = job.get("statistics", {}).get("query", {})
        if metadata["job_state"] == "DONE":
            break
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    records, page_token, schema, seen_tokens = [], None, None, set()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            result.update(status="pending", ok=False, rows=[])
            metadata["resume_required"] = True
            return _finish(result, client)
        params = {"location": location, "maxResults": 10000, "timeoutMs": 0}
        if page_token:
            params["pageToken"] = page_token
        path = "/projects/" + urllib.parse.quote(project, safe="") + "/queries/" + urllib.parse.quote(job_id, safe="")
        response = client.request("GET", path, params=params, timeout=min(20, max(1, remaining)))
        if not response.get("jobComplete"):
            result.update(status="pending", ok=False, rows=[])
            metadata["resume_required"] = True
            return _finish(result, client)
        schema = response.get("schema") or schema
        if not schema:
            raise SourceError("error", "invalid_result", "Completed query did not provide a schema")
        if int(response.get("totalRows", "0")) > MAX_RESULT_ROWS:
            raise SourceError("blocked", "result_limit", "Query result exceeds 100,000 aggregate rows")
        records.extend(_decode_rows(schema, response.get("rows", [])))
        page_token = response.get("pageToken")
        if page_token and page_token in seen_tokens:
            raise SourceError("error", "invalid_result", "BigQuery repeated a pagination token")
        if page_token:
            seen_tokens.add(page_token)
        if not page_token:
            if len(records) != int(response.get("totalRows", len(records))):
                raise SourceError("error", "invalid_result", "Result pagination did not return every row")
            break
    result.update(ok=True, status="ok", rows=records, error=None)
    metadata.update(resume_required=False, row_count=len(records), result_schema=schema)
    result["coverage"].update(
        query_completed=True, missing_rows_are_zero=True,
        zero_meaning="No matching records in the successfully queried source; platform completeness is not guaranteed",
        source_completeness="not_independently_guaranteed",
    )
    if result["source"] == "gharchive":
        missing = sum((row.get("missing_event_id_rows") or 0) for row in records
                      if row.get("event_type") != "__all_types__")
        result["coverage"]["missing_event_id_rows"] = missing
        result["coverage"]["missing_rows_are_zero"] = missing == 0
    return _finish(result, client)


def fetch_aggregate(source, project, tools, start, end_exclusive, max_bytes_billed,
                    credentials=None, *, dry_run_only=False, job_id=None,
                    location="US", poll_timeout=45):
    """Collect one to seven completed UTC days, or return blocked/error/pending.

    credentials accepts a bearer token string, google-auth credentials, or an
    AuthorizedSession-compatible request object. None uses ADC lazily. Secrets
    are never included in result metadata. dry_run_only validates without paid
    execution. Stable job IDs make interrupted calls resumable without submitting
    another job; pass a distinct explicit job_id for a deliberate fresh snapshot.
    """
    result = _base(source, project, location)
    client = None
    try:
        plan = build_query(source, tools, start, end_exclusive)
        result["source"] = plan["source"]
        result["query_sql"] = plan["query_sql"]
        result["request_metadata"].update({key: plan[key] for key in ("start", "end_exclusive", "tools", "query_parameters")})
        if plan["source"] == "gharchive":
            start_day, end_day = _date(plan["start"]), _date(plan["end_exclusive"])
            result["request_metadata"].update(
                source_fields=["id", "created_at", "type", "actor.id"]
                    + (["repo.id"] if any(item["repo_ids"] for item in plan["tools"]) else [])
                    + (["repo.name"] if any(not item["repo_ids"] for item in plan["tools"]) else []),
                payload_fields_collected=False,
                action_status="not_collected; empty daily action is unknown, not an action category",
                merged_pr_events_status="not_collected; NULL is not zero merged pull requests",
                daily_grain="tool/date/event_type",
                window_unique_actor_marker={
                    "event_type": "__all_types__", "action": "__window_unique_actors__",
                    "start": plan["start"], "end_exclusive": plan["end_exclusive"],
                    "complete_monday_week": start_day.weekday() == 0 and (end_day - start_day).days == 7,
                    "meaning": "Distinct valid-event actors across this exact request window; do not sum daily distinct actors",
                },
            )
        result["coverage"].update(start=plan["start"], end_exclusive=plan["end_exclusive"], tool_ids=[item["tool_id"] for item in plan["tools"]])
        if not project:
            raise SourceError("blocked", "missing_project", "An explicit Google Cloud query project is required")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,127}", str(project)):
            raise ValueError("Invalid query project identifier")
        if isinstance(max_bytes_billed, bool) or not isinstance(max_bytes_billed, int) or max_bytes_billed <= 0:
            raise ValueError("max_bytes_billed must be an explicit positive integer")
        query = {"query": plan["query_sql"], "useLegacySql": False, "parameterMode": "NAMED",
                 "queryParameters": plan["query_parameters"], "maximumBytesBilled": str(max_bytes_billed),
                 "priority": "BATCH", "useQueryCache": True}
        fingerprint = hashlib.sha256(_encode({"project": project, "location": location, "query": query}).encode()).hexdigest()
        job_id = job_id or "radar_" + plan["source"] + "_" + fingerprint[:40]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,1024}", job_id):
            raise ValueError("Invalid BigQuery job_id")
        result["request_metadata"].update(maximum_bytes_billed=max_bytes_billed, query_fingerprint=fingerprint,
                                          priority="BATCH", dry_run_only=bool(dry_run_only))
        client = _Client(credentials)
        if not dry_run_only:
            existing = client.request("GET", _path(project, job_id), params={"location": location}, allow_404=True)
            if existing is not None:
                _check_query_match(existing, result)
                result["job_id"] = job_id
                result["request_metadata"]["resumed_existing_job"] = True
                return _poll(client, result, poll_timeout)
        result["coverage"]["verified_source_days"] = _check_tables(client, plan)
        dry_query = {key: value for key, value in query.items() if key not in ("maximumBytesBilled", "priority")}
        dry_job = client.request("POST", _path(project), {"jobReference": {"projectId": project, "location": location},
                                                        "configuration": {"dryRun": True, "query": dry_query}})
        _check_job_error(dry_job)
        estimate = dry_job.get("statistics", {}).get("totalBytesProcessed")
        if estimate is None:
            estimate = dry_job.get("statistics", {}).get("query", {}).get("totalBytesProcessed")
        if estimate is None:
            raise SourceError("blocked", "missing_dry_run_estimate", "Dry run did not return a scan estimate")
        estimate = int(estimate)
        result["request_metadata"]["estimated_bytes_processed"] = estimate
        if estimate > max_bytes_billed:
            raise SourceError("blocked", "bytes_cap_exceeded", f"Dry-run estimate {estimate} exceeds cap {max_bytes_billed}; no query was executed")
        if dry_run_only:
            result.update(status="dry_run_ok", ok=True)
            return _finish(result, client)
        result["job_id"] = job_id
        body = {"jobReference": {"projectId": project, "jobId": job_id, "location": location},
                "configuration": {"query": query, "labels": {"application": "ai_technology_radar", "source": plan["source"]}}}
        job = client.request("POST", _path(project), body, allow_409=True)
        if job is not None:
            _check_job_error(job)
        result["request_metadata"]["submitted_job_id"] = job_id
        return _poll(client, result, poll_timeout)
    except SourceError as error:
        return _failed(result, error, client)
    except (ValueError, TypeError, KeyError) as error:
        return _failed(result, SourceError("error", "invalid_input_or_response", str(error)), client)


def resume_aggregate(previous, credentials=None, *, poll_timeout=45):
    """Poll a saved pending result. This function never submits a query job."""
    result = deepcopy({key: value for key, value in previous.items() if key != "raw_bytes"})
    result["raw_bytes"] = b""
    client = None
    try:
        if not result.get("job_id") or not result.get("request_metadata", {}).get("project"):
            raise SourceError("blocked", "missing_job_reference", "A job_id and query project are required to resume")
        client = _Client(credentials)
        return _poll(client, result, poll_timeout)
    except SourceError as error:
        return _failed(result, error, client)
    except (ValueError, TypeError, KeyError) as error:
        return _failed(result, SourceError("error", "invalid_input_or_response", str(error)), client)
