"""Apply a new feature policy to verified normalized observations, without HTTP.

The immutable normalization and its code hashes remain separate from this later
feature-policy transformation. Original records and source evidence are retained.
"""
import argparse,csv,gzip,json,shutil,tempfile
from collections import defaultdict
from datetime import datetime,timezone
from pathlib import Path
import build_github_activity as b


def reframe(source,output):
    source=Path(source).resolve();output=Path(output)
    if output.exists():raise FileExistsError('Use a fresh immutable feature-policy run')
    manifest=json.loads((source/'manifest.json').read_text());quality=json.loads((source/'source_quality.json').read_text())
    if manifest.get('source')!=b.SOURCE or quality.get('source')!=b.SOURCE:raise ValueError('Verified normalization source type differs')
    for name,expected in manifest['files'].items():
        p=(source/name).resolve()
        if not p.is_relative_to(source) or not p.is_file() or b.sha(p)!=expected:raise ValueError('Verified normalization file hash/path differs')
    registry=json.loads((source/'tools.study.json').read_text());tools={t['tool_id']:t for t in registry['tools']}
    if b.sha(source/'tools.study.json')!=manifest['registry_sha256']:raise ValueError('Verified normalization cohort differs')
    records={}
    with gzip.open(source/'normalized_records.jsonl.gz','rt') as f:
        for line in f:
            row=json.loads(line);key=(row['tool_id'],row['repo_id'],row['number'])
            if row['tool_id'] not in tools or row['repo_id']!=tools[row['tool_id']]['github_repo_id'] or key in records:raise ValueError('Normalized canonical identity is invalid or duplicated')
            if len(records)>=2000000:raise ValueError('Normalized record bound exceeded')
            row['_view_records']=row['source_view_observations'];records[key]=row
    diagnostics=quality['diagnostics'];core=quality['core_pr_quality'];intervals=defaultdict(list)
    for leaf in diagnostics['partition_plan']['leaves']:
        if not leaf.get('count_only') and diagnostics['leaf_checks'][leaf['search_job_id']]['complete']:
            intervals[(leaf['tool_id'],leaf['stream'])].append((b.utc(leaf['window_start']),b.utc(leaf['window_end_exclusive'])))
    rows=b.weekly_features(records,intervals,core,registry,quality['history_start'],quality['history_end_exclusive'],True,diagnostics['visible_list_quality'],diagnostics.get('native_pr_history_quality'))
    captured=all(q['visible_list_pagination_complete'] for q in diagnostics['visible_list_quality'].values()) and all(q['created_visible_offset_crawl_complete'] for q in core.values())
    if diagnostics.get('native_pr_history',{}).get('configured'):captured=captured and all(q['capture_complete'] for q in diagnostics['native_pr_history_quality'].values())
    review=any(not r['coverage_complete'] for r in rows) or any(q.get('event_time_anomalies') for q in diagnostics.get('native_pr_history_quality',{}).values());status='needs_review' if captured and review else 'passed' if captured else 'partial'
    output.parent.mkdir(parents=True,exist_ok=True);stage=Path(tempfile.mkdtemp(prefix='.github-reframe-',dir=output.parent))
    try:
        for name in ['normalized_records.jsonl.gz','collection_state.snapshot.json','tools.study.json']:shutil.copyfile(source/name,stage/name)
        shutil.copyfile(source/'manifest.json',stage/'normalization_manifest.json');shutil.copyfile(source/'source_quality.json',stage/'normalization_source_quality.json')
        for name,group in [('github_activity_weekly.csv',[r for r in rows if r['week_complete']]),('github_activity_partial_weeks.csv',[r for r in rows if not r['week_complete']])]:
            with (stage/name).open('w',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=list(rows[0]) if rows else ['tool_id','week_start']);writer.writeheader();writer.writerows(group)
        updated={**quality,'status':status,'collection_complete':captured,'built_at':datetime.now(timezone.utc).isoformat(),
            'weekly_rows':sum(r['week_complete'] for r in rows),'coverage_complete_rows':sum(r['week_complete'] and r['coverage_complete'] for r in rows),
            'normalization_input_path':str(source),'normalization_manifest_sha256':b.sha(source/'manifest.json'),
            'normalization_code_sha256':quality.get('normalization_code_sha256',quality['code_sha256']),
            'feature_policy_code_sha256':{Path(__file__).name:b.sha(Path(__file__)),'build_github_activity.py':b.sha(Path(b.__file__))},
            'capture_and_feature_review_separate':True,
            'limits':[v for v in quality.get('limits',[]) if not v.startswith('Independent complete core-PR and visible-list created-ID disagreement masks exact PR features')] + [
                'Independently verified core PR-created counts and account counts are preserved even when a completed issue-list profile disagrees.',
                'Hybrid merge/closure and union-author features remain conservatively masked when independent source profiles disagree.',
                'Issue-list coverage describes the actual provider resultset for its retained filter; JSON timestamps are not asserted to equal internal filter keys.'],
            'feature_policy':'independent core PR-created cohort; conservative hybrid view agreement; all historical primary eligibility false'}
        for name in ['source_quality.json','github_activity_quality.json']:(stage/name).write_text(json.dumps(updated,indent=2)+'\n')
        final={**{k:v for k,v in manifest.items() if k!='files'},'status':status,'collection_complete':captured,
            'normalization_manifest_sha256':updated['normalization_manifest_sha256'],
            'files':{p.name:b.sha(p) for p in stage.iterdir() if p.is_file()}}
        (stage/'manifest.json').write_text(json.dumps(final,indent=2)+'\n');stage.rename(output)
    except BaseException:shutil.rmtree(stage,ignore_errors=True);raise
    return updated

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();q=reframe(a.input,a.output)
    print(json.dumps({'output':str(a.output),'status':q['status'],'capture_complete':q['collection_complete'],'weekly_rows':q['weekly_rows']}))
