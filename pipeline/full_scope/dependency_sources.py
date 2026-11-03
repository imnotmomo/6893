"""Immutable PyPI metadata query plans and lossless dependency-row decoding.

No query is submitted by this module. The caller must use the project's shared
BigQuery reservation ledger, dry-run checks and maximum-bytes-billed cap.
"""
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

TABLE = 'bigquery-public-data.pypi.distribution_metadata'
SOURCE = 'pypi_immutable_distribution_metadata'
FIELDS = ('name', 'version', 'metadata_version', 'summary', 'requires_dist',
          'requires_python', 'classifiers', 'project_urls', 'upload_time',
          'filename', 'path', 'sha256_digest', 'size', 'python_version', 'packagetype')
DOCUMENTATION = 'https://docs.pypi.org/api/bigquery/'


def canonical(name):
    value = re.sub(r'[-_.]+', '-', str(name).strip()).lower()
    if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', value):
        raise ValueError('Invalid Python distribution name')
    return value


def targets(registry):
    items = registry['tools'] if isinstance(registry, dict) else registry
    mapping = {canonical(item['pypi_package']): item['tool_id'] for item in items}
    if not mapping or len(mapping) != len(items):
        raise ValueError('Missing or duplicate canonical target distributions')
    return mapping


def build_metadata_query(registry, end_exclusive, mode='candidate_history'):
    """Return all artifact versions, including releases dropping a target.

    Discovery spans retained metadata before end, including the pre-2023
    baseline. Results are file-level and preserve all Requires-Dist strings.
    No author names/emails, artifact binaries or descriptions are selected.
    """
    end = date.fromisoformat(str(end_exclusive))
    if end > datetime.now(timezone.utc).date():
        raise ValueError('Exclude incomplete/future UTC dates')
    mapping = targets(registry)
    selected = ', '.join(FIELDS)
    name = "REGEXP_REPLACE(LOWER(name), r'[-_.]+', '-')"
    requirement = "REGEXP_REPLACE(LOWER(REGEXP_EXTRACT(requirement, r'^\\s*([A-Za-z0-9][A-Za-z0-9._-]*)')), r'[-_.]+', '-')"
    source = f"""WITH source AS (
  SELECT {selected}
  FROM `{TABLE}`
  WHERE upload_time < TIMESTAMP(@end_date, 'UTC') OR upload_time IS NULL
), candidates AS (
  SELECT DISTINCT {name} AS package
  FROM source
  WHERE {name} IN UNNEST(@target_packages)
     OR EXISTS (
       SELECT 1 FROM UNNEST(requires_dist) AS requirement
       WHERE {requirement} IN UNNEST(@target_packages)
     )
)
"""
    if mode == 'candidate_history':
        sql = source + f"""SELECT source.*
FROM source
JOIN candidates ON {name} = candidates.package
ORDER BY name, version, upload_time, filename, path"""
    elif mode == 'candidate_names':
        sql = source + 'SELECT package FROM candidates ORDER BY package'
    elif mode == 'outbound_only':
        sql = f"""SELECT {selected}
FROM `{TABLE}`
WHERE ({name} IN UNNEST(@target_packages))
  AND (upload_time < TIMESTAMP(@end_date, 'UTC') OR upload_time IS NULL)
ORDER BY name, version, upload_time, filename, path"""
    else:
        raise ValueError('mode must be candidate_history, candidate_names or outbound_only')
    parameters = [
        {'name': 'end_date', 'parameterType': {'type': 'DATE'},
         'parameterValue': {'value': end.isoformat()}},
        {'name': 'target_packages', 'parameterType': {'type': 'ARRAY', 'arrayType': {'type': 'STRING'}},
         'parameterValue': {'arrayValues': [{'value': name} for name in sorted(mapping)]}},
    ]
    fingerprint = hashlib.sha256(json.dumps({'query': sql, 'parameters': parameters}, sort_keys=True).encode()).hexdigest()
    return {'source': SOURCE, 'table': TABLE, 'query': sql, 'query_parameters': parameters,
            'end_exclusive': end.isoformat(), 'mode': mode, 'query_sha256': fingerprint,
            'target_package_to_tool': mapping, 'row_grain': 'uploaded distribution artifact metadata',
            'pre_study_history_required': True, 'requires_shared_budget_ledger': True,
            'historical_policy': 'Retrospective immutable upload-event reconstruction; observed_at is collection time, not a real-time historical vintage.',
            'documentation': DOCUMENTATION}


def decode_rows(schema, rows):
    """Decode REST repeated and nested cells rather than losing arrays."""
    def scalar(field, value):
        if value is None:
            return None
        kind = field['type']
        if kind in ('RECORD', 'STRUCT'):
            return record(field['fields'], value)
        if kind in ('INTEGER', 'INT64'):
            return int(value)
        if kind in ('FLOAT', 'FLOAT64'):
            return float(value)
        if kind in ('BOOLEAN', 'BOOL'):
            if value not in (True, False, 'true', 'false', 'True', 'False'):
                raise ValueError('Invalid BigQuery Boolean')
            return value is True or str(value).lower() == 'true'
        return value
    def cell(field, value):
        if field.get('mode') == 'REPEATED':
            if value is None:
                return []
            if not isinstance(value, list):
                raise ValueError('Repeated BigQuery field must be an array')
            return [scalar(field, entry.get('v')) for entry in value]
        return scalar(field, value)
    def record(fields, row):
        cells = row.get('f', [])
        if len(cells) != len(fields):
            raise ValueError('BigQuery schema width differs from returned row')
        return {field['name']: cell(field, item.get('v')) for field, item in zip(fields, cells)}
    fields = schema.get('fields', [])
    if not fields:
        raise ValueError('BigQuery response has no schema')
    return [record(fields, row) for row in rows]


def utc(value):
    """Accept provider ISO timestamps and exact REST Unix-second strings."""
    if value is None:
        return None
    if isinstance(value, datetime):
        timestamp = value
    elif re.fullmatch(r'[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?', str(value)):
        from datetime import timedelta
        seconds = Decimal(str(value))
        timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(seconds * 1000000))
    else:
        timestamp = datetime.fromisoformat(str(value).replace('Z', '+00:00').replace(' UTC', '+00:00'))
    if timestamp.tzinfo is None:
        raise ValueError('Provider timestamp lacks a timezone')
    return timestamp.astimezone(timezone.utc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--end-exclusive', required=True)
    parser.add_argument('--mode', choices=['candidate_history', 'candidate_names', 'outbound_only'], default='candidate_history')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plan = build_metadata_query(json.loads(args.registry.read_text()), args.end_exclusive, args.mode)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, indent=2) + '\n')
    print(json.dumps({'status': 'query_plan_only', 'output': str(args.output), 'query_sha256': plan['query_sha256']}))


if __name__ == '__main__':
    main()
