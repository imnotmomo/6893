"""Classify deliberately unsubmitted rich-query parents from verified leaves.

The feature builder has already validated each completed query's cohort,
parameters, receipt and body hash. This module never fetches data or writes it.
"""
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path


SUPERSEDED = 'superseded_by_complete_child_queries'


def classify_split_parents(intervals, queries, winners):
    result = []
    for original in intervals:
        row = dict(original)
        folder = Path(row['path'])
        marker = folder / 'split_children.json'
        if not marker.is_file():
            result.append(row)
            continue
        raw = marker.read_bytes()
        split = json.loads(raw)
        manifest_raw = (folder / 'manifest.json').read_bytes()
        manifest = json.loads(manifest_raw)
        left, right = date.fromisoformat(row['start']), date.fromisoformat(row['end_exclusive'])
        if (right-left).days <= 1:
            raise ValueError('A split parent must contain at least two UTC dates')
        middle = left + timedelta(days=(right-left).days//2)
        expected = [[str(left), str(middle)], [str(middle), str(right)]]
        fingerprint = row['query_sha256']
        source_plan = manifest.get('source_plan', {})
        estimate = split.get('estimated_scan_bytes')
        billed = manifest.get('actual_billed_bytes')
        if (split.get('query_sha256') != fingerprint or split.get('windows') != expected
                or manifest.get('query_sha256') != fingerprint
                or source_plan.get('query_sha256') != fingerprint
                or source_plan.get('start') != row['start']
                or source_plan.get('end_exclusive') != row['end_exclusive']
                or manifest.get('source') != 'pypi_download_dimensions'
                or manifest.get('status') != 'split' or manifest.get('reserved_at')
                or manifest.get('job_complete') is not False
                or manifest.get('all_pages_fetched') is not False
                or manifest.get('pages') != []
                or type(manifest.get('rows_fetched')) is not int or manifest['rows_fetched'] != 0
                or (billed is not None and (type(billed) is not int or billed != 0))
                or type(estimate) is not int or estimate <= 0
                or (folder / 'cloud_job.json').exists()
                or (folder / 'completion_receipt.json').exists()):
            raise ValueError('Split-parent evidence differs from its unsubmitted frozen plan')
        # A whole parent copied elsewhere is not evidence of completed children.
        # Every winning date must come from a query wholly inside one declared
        # child interval, including recursively split grandchildren.
        descendants = []
        for query in queries:
            a, b = date.fromisoformat(query['start']), date.fromisoformat(query['end_exclusive'])
            if a < b and any(date.fromisoformat(x) <= a < b <= date.fromisoformat(y) for x, y in expected):
                descendants.append(query)
        paths = {query['input_path'] for query in descendants}
        missing = [str(left + timedelta(days=n)) for n in range((right-left).days)
                   if winners.get(left + timedelta(days=n)) not in paths]
        row.update(state='split_parent_children_incomplete' if missing else SUPERSEDED,
                   split_children_sha256=hashlib.sha256(raw).hexdigest(),
                   split_parent_manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
                   child_windows=expected,
                   completed_descendant_query_sha256=sorted({q['query_sha256'] for q in descendants}),
                   missing_child_query_days=missing)
        if marker.read_bytes() != raw or (folder / 'manifest.json').read_bytes() != manifest_raw:
            raise ValueError('Split-parent evidence changed during classification')
        result.append(row)
    return result
