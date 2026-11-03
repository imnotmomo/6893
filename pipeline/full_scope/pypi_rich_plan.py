"""Plans for richer download observations; no requests or paid queries here."""
import hashlib
import json
from datetime import date


def plan(registry, start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if not start < end:
        raise ValueError('Empty download interval')
    mapping = {tool['pypi_package']: tool['tool_id'] for tool in registry['tools']}
    if len(mapping) != len(registry['tools']):
        raise ValueError('Duplicate distribution mapping')
    sql = r'''SELECT project AS package, DATE(timestamp, 'UTC') AS date,
 file.version AS package_version, file.type AS artifact_type,
 REGEXP_EXTRACT(details.python, r'^([0-9]+\.[0-9]+)') AS python_major_minor,
 details.implementation.name AS python_implementation,
 details.system.name AS operating_system,
 details.installer.name AS installer,
 country_code,
 CASE WHEN details.ci IS TRUE THEN 'reported_true'
      WHEN details.ci IS FALSE THEN 'reported_false' ELSE 'unknown' END AS ci_report,
 http.status_code AS http_status,
 COUNT(*) AS logged_objects,
 COUNTIF(REGEXP_CONTAINS(file.filename, r'\.(whl|tar\.gz|zip)$')) AS downloads,
 COUNT(*) - COUNTIF(REGEXP_CONTAINS(file.filename, r'\.(whl|tar\.gz|zip)$')) AS excluded_requests
FROM `bigquery-public-data.pypi.file_downloads`
WHERE timestamp >= TIMESTAMP(@start_date, 'UTC')
  AND timestamp < TIMESTAMP(@end_date, 'UTC')
  AND project IN UNNEST(@packages)
GROUP BY package, date, package_version, artifact_type, python_major_minor,
 python_implementation, operating_system, installer, country_code, ci_report, http_status
ORDER BY date, package, package_version, artifact_type, python_major_minor,
 python_implementation, operating_system, installer, country_code, ci_report, http_status'''
    params = [
        {'name':'start_date','parameterType':{'type':'DATE'},'parameterValue':{'value':str(start)}},
        {'name':'end_date','parameterType':{'type':'DATE'},'parameterValue':{'value':str(end)}},
        {'name':'packages','parameterType':{'type':'ARRAY','arrayType':{'type':'STRING'}},
         'parameterValue':{'arrayValues':[{'value':v} for v in sorted(mapping)]}},
    ]
    return {'source':'pypi_download_dimensions','query':sql,'query_parameters':params,
            'start':str(start),'end_exclusive':str(end),'package_to_tool':mapping,
            'row_grain':'package/day/version/environment/country/reported-CI/HTTP-status aggregate',
            'measurement':'Artifact requests with the original filename filter; not installations, people, retention, or a reliable CI detector.',
            'missing_dimension_policy':'Provider nulls are unknown, never interpreted as a real platform or non-CI use.',
            'query_sha256':hashlib.sha256(json.dumps({'query':sql,'parameters':params},sort_keys=True).encode()).hexdigest()}
