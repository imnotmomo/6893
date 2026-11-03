"""Classify retained immutable-metadata conflicts without reprocessing edges.

This is read-only. It preserves every provider claim and distinguishes metadata
variance from possible ambiguity in the frozen cohort's actual counters.
"""
import argparse
from collections import Counter,defaultdict
from functools import lru_cache
from datetime import datetime,timedelta,timezone
import gzip,hashlib,json
from pathlib import Path

from packaging.specifiers import SpecifierSet,InvalidSpecifier
from build_dependency_features import encode,requirement,sha
from dependency_sources import FIELDS,SOURCE,targets


@lru_cache(maxsize=32768)
def parsed(value):return requirement(value)

def signature(item):
    values=[];members=set();invalid=0
    for value in item['requires_dist']:
        req=parsed(value) if isinstance(value,str) else requirement(value)
        if req['valid']:
            members.add(req['dependency_package'])
            values.append(encode({key:req[key] for key in ['dependency_package','specifier','extras','environment_marker','direct_url']}))
        else:invalid+=1;values.append(encode(req))
    try:python=('valid',str(SpecifierSet(item.get('requires_python') or '')))
    except InvalidSpecifier:python=('invalid',item.get('requires_python'))
    provider={name:item.get(name) for name in FIELDS}
    return {'artifact_id':item['artifact_id'],'package':item['package'],'version':item['version'],
        'provider_upload_time':item['provider_upload_time'],'identity_valid':item['identity_valid'],
        'provider_hash_verified':hashlib.sha256(encode(provider).encode()).hexdigest()==item['artifact_id'],
        'field_hashes':{name:hashlib.sha256(encode(value).encode()).hexdigest() for name,value in provider.items()},
        'requirement_semantics':frozenset(values),'members':members,'requires_python':python,'invalid_requirements':invalid,
        'valid_requirement_semantics':frozenset(value for value in values if json.loads(value).get('dependency_package'))}


def classify(records,mapping,selected_weeks):
    package=records[0]['package'];version=records[0]['version']
    changed=[name for name in FIELDS if len({r['field_hashes'][name] for r in records})>1]
    semantics_equal=len({r['requirement_semantics'] for r in records})==1
    python_equal=len({r['requires_python'] for r in records})==1
    memberships=[r['members']&set(mapping)-{package} if r['identity_valid'] else set() for r in records]
    union=set().union(*memberships);intersection=set.intersection(*memberships) if memberships else set()
    ambiguous=sorted(union-intersection)
    selected=selected_weeks.get((package,version),[]) if package in mapping else []
    dates=[datetime.fromisoformat(r['provider_upload_time'].replace('Z','+00:00')) for r in records if r['provider_upload_time']]
    if dates and len(dates)==len(records):
        selected=[week for week in selected if min(dates)<datetime.fromisoformat(week).replace(tzinfo=timezone.utc)+timedelta(days=7)]
    valid_semantic_equal=len({r['valid_requirement_semantics'] for r in records})==1
    candidate_outbound=bool(selected and not valid_semantic_equal)
    category=('possible_cohort_feature_ambiguity' if ambiguous or candidate_outbound else
              'foreign_requirement_variance' if not semantics_equal else
              'python_constraint_variance_without_counter_change' if not python_equal else 'non_feature_metadata_variance')
    return {'package':package,'version':version,'artifact_ids':[r['artifact_id'] for r in records],
        'provider_upload_times':sorted({r['provider_upload_time'] for r in records if r['provider_upload_time']}),
        'changed_provider_fields':changed,'requires_dist_semantic_equal':semantics_equal,'requires_python_semantic_equal':python_equal,
        'cohort_target_membership_by_variant':[sorted(value) for value in memberships],
        'candidate_inbound_targets':ambiguous,'candidate_outbound_selected_weeks':selected if candidate_outbound else [],
        'classification':category,'all_provider_hashes_verified':all(r['provider_hash_verified'] for r in records),
        'note':'Candidates require complete per-package artifact context before any feature mask; conditional declarations all count as membership.'}


def audit(source,registry):
    import csv
    source=Path(source).resolve();manifest=json.loads((source/'manifest.json').read_text());mapping=targets(json.loads(Path(registry).read_text()))
    if manifest.get('source')!=SOURCE or manifest.get('registry_sha256')!=sha(Path(registry)):raise ValueError('Source/cohort differs')
    for name in ['parse_errors.jsonl.gz','artifacts.jsonl.gz','weekly_dependency_features.csv']:
        if manifest.get('files',{}).get(name)!=sha(source/name):raise ValueError('Source file hash differs: '+name)
    parents={};edges=[]
    def find(value):
        parents.setdefault(value,value)
        if parents[value]!=value:parents[value]=find(parents[value])
        return parents[value]
    with gzip.open(source/'parse_errors.jsonl.gz','rt') as stream:
        for line in stream:
            item=json.loads(line)
            if item.get('kind')!='conflicting_artifact_metadata':continue
            left,right=item['raw_row_sha256'],item['other_raw_row_sha256'];parents[find(left)]=find(right);edges.append((left,right))
    wanted=set(parents);records={};scanned=0
    with gzip.open(source/'artifacts.jsonl.gz','rt') as stream:
        for line in stream:
            item=json.loads(line);scanned+=1
            if item['artifact_id'] in wanted:records[item['artifact_id']]=signature(item)
    if set(records)!=wanted:raise ValueError('Conflict evidence references an absent artifact')
    selected=defaultdict(list)
    with (source/'weekly_dependency_features.csv').open() as stream:
        for row in csv.DictReader(stream):
            version=row.get('dep_latest_uploaded_target_version')
            if version:selected[(row['package'],version)].append(row['week_start'])
    groups=defaultdict(list)
    for key,value in records.items():groups[find(key)].append(value)
    details=[classify(sorted(values,key=lambda r:r['artifact_id']),mapping,selected) for values in groups.values()]
    result={'source':SOURCE,'audit_scope':'all retained conflict groups; no provider rows/feature flags modified',
        'source_manifest_sha256':sha(source/'manifest.json'),'registry_sha256':sha(Path(registry)),
        'artifacts_scanned':scanned,'conflict_pairs':len(edges),'conflict_groups':len(details),
        'classifications':dict(Counter(row['classification'] for row in details)),
        'candidate_inbound_targets':sorted({name for row in details for name in row['candidate_inbound_targets']}),
        'candidate_selected_outbound_packages':sorted({row['package'] for row in details if row['candidate_outbound_selected_weeks']}),
        'all_provider_hashes_verified':all(row['all_provider_hashes_verified'] for row in details),'groups':details}
    if sha(source/'manifest.json')!=result['source_manifest_sha256']:raise ValueError('Source changed during audit')
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--registry',type=Path,required=True);p.add_argument('--output',type=Path)
    args=p.parse_args();result=audit(args.source,args.registry)
    if args.output:
        if args.output.exists():raise FileExistsError('Use a fresh audit output path')
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='groups'}))


if __name__=='__main__':main()
