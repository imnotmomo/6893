"""Resume immutable dependency exports, then refresh once per completed week.

Every query uses full_scope_bq and the original cumulative reservation ledger.
No completed export is re-bought; retries reuse its deterministic cloud job.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys

from dependency_sources import build_metadata_query
from review_dependency_conflicts import reviewed_source

ROOT=Path.home()/'6893'


def endpoint(today, initial_end):
    stop=min(today,date(2027,1,1))
    weekly=stop if stop==date(2027,1,1) else stop-timedelta(days=stop.weekday())
    return max(initial_end,weekly)


def atomic(path,value):
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_text(json.dumps(value,indent=2,default=str)+'\n');os.replace(temporary,path)


def finalize_latest(plans,data,registry):
    complete=[]
    for plan_path,folder in plans:
        receipt_path=folder/'completion_receipt.json'
        if not receipt_path.exists():continue
        receipt=json.loads(receipt_path.read_text())
        manifest=json.loads((folder/'manifest.json').read_text())
        if (manifest.get('status')=='complete' and receipt.get('all_pages_fetched') is True
                and receipt.get('job_complete') is True):
            complete.append((plan_path,folder,receipt))
    if not complete:return
    plan_path,folder,receipt=complete[-1];definition=json.loads(plan_path.read_text())
    endpoint=definition['end_exclusive']
    output=data/'full_scope/dependency_history_runs'/('-'.join([endpoint,receipt['query_sha256'][:12],receipt['output_sha256'][:12]]))
    output.parent.mkdir(parents=True,exist_ok=True)
    if not output.exists():
        result=subprocess.run([sys.executable,str(Path(__file__).with_name('build_dependency_features_streaming.py')),
            '--input',str(folder/'decoded_records.jsonl.gz'),'--registry',str(registry),
            '--query-plan',str(plan_path),'--query-receipt',str(folder/'completion_receipt.json'),
            '--output',str(output),'--end-exclusive',endpoint])
        if result.returncode:raise ValueError('Dependency features did not validate; previous publication retained')
    from assemble_full_scope import inspect_source,sha
    quality=json.loads((output/'dependency_quality.json').read_text())
    if (quality.get('query_receipt_sha256')!=sha(folder/'completion_receipt.json')
            or quality.get('query_plan_sha256')!=sha(plan_path)
            or quality.get('input_metadata_sha256')!=receipt['output_sha256']):
        raise ValueError('Dependency feature output is bound to another query receipt')
    # Keep the immutable base graph, but always adopt the deterministic reviewed
    # version. Repeated cron runs cannot roll back exact conflict-week masks.
    output=reviewed_source(output,registry)
    ids={tool['tool_id'] for tool in json.loads(registry.read_text())['tools']}
    # Reuse the joined-spine provenance gate before moving the source pointer.
    # Source validity flags remain independent of retrieval completion.
    features,catalog,_=inspect_source('dependency',output,sha(registry),ids)
    if features is None or catalog.get('retrieval_status')=='awaiting_weekly_features':
        raise ValueError('Dependency feature output lacks validated weekly observations; previous publication retained')
    pointer=data/'full_scope/dependency_history'
    if pointer.exists() and not pointer.is_symlink():
        if pointer.resolve()==output.resolve():return output
        raise ValueError('Canonical dependency path is a directory; preserve it before adopting immutable pointers')
    temporary=pointer.with_name('.dependency_history.next')
    if temporary.is_symlink():temporary.unlink()
    temporary.symlink_to(output);os.replace(temporary,pointer)
    return output


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-seconds',type=int,default=600)
    args=parser.parse_args()
    if not 1<=args.max_seconds<=1800:parser.error('Use a bounded runtime')
    data=ROOT/'data';archive=data/'raw/full_scope/bigquery';archive.mkdir(parents=True,exist_ok=True)
    with (archive/'.metadata-dispatch.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'source':'pypi_immutable_distribution_metadata','status':'skipped_collection_busy',
                              'lock':'metadata-dispatch'}))
            return 0
        initial_path=ROOT/'config/full_scope/metadata_history.json'
        initial=json.loads(initial_path.read_text());initial_end=date.fromisoformat(initial['end_exclusive'])
        registry=ROOT/'config/tools.study.json'
        today=datetime.now(timezone.utc).date()
        plans=[(initial_path,archive/'metadata-history')]
        target=endpoint(today,initial_end)
        if target>initial_end:
            plan=build_metadata_query(json.loads(registry.read_text()),str(target))
            path=ROOT/'config/full_scope'/('metadata-'+str(target)+'.json')
            if path.exists() and json.loads(path.read_text())!=plan:raise ValueError('Frozen metadata plan changed')
            if not path.exists():atomic(path,plan)
            plans.append((path,archive/('metadata-'+str(target))))
        for plan_path,out in plans:
            manifest_path=out/'manifest.json'
            if manifest_path.exists() and json.loads(manifest_path.read_text()).get('status')=='complete':
                continue
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('full_scope_bq.py')),
                '--plan',str(plan_path),'--output',str(out),'--execute',
                '--max-seconds',str(args.max_seconds),'--page-size','100000'])
            manifest=json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
            atomic(data/'full_scope_metadata_status.json',{'source':'pypi_immutable_distribution_metadata',
                'updated_at':datetime.now(timezone.utc).isoformat(),'end_exclusive':json.loads(plan_path.read_text())['end_exclusive'],
                'export_path':str(out),'status':manifest.get('status','failed'),
                'rows_fetched':manifest.get('rows_fetched'),'total_rows':manifest.get('total_rows'),
                'exit_code':result.returncode,'refresh_cadence':'weekly after initial backfill'})
            if result.returncode==0 and manifest.get('status')=='complete':finalize_latest(plans,data,registry)
            return result.returncode
        published=finalize_latest(plans,data,registry)
        quality=json.loads((published/'dependency_quality.json').read_text()) if published else {}
        status={'source':'pypi_immutable_distribution_metadata',
            'updated_at':datetime.now(timezone.utc).isoformat(),'status':'complete',
            'end_exclusive':str(target),'export_path':str(plans[-1][1]),'refresh_cadence':'weekly after initial backfill',
            'collection_complete':quality.get('collection_complete',False),
            'input_query_complete':quality.get('input_query_complete',False),
            'quality_status':quality.get('quality_status',quality.get('status')),
            'feature_source_path':str(published) if published else None,
            'query_submitted':False,'weekly_rows':quality.get('weekly_rows')}
        atomic(data/'full_scope_metadata_status.json',status)
        print(json.dumps(status))
        return 0


if __name__=='__main__':raise SystemExit(main())
