"""Add cross-tool v1 alias candidates using retained papers only, with no HTTP.

The original query universe, provider responses and observation vintage remain
unchanged. A match creates one review candidate per tool/paper, never a verified
tool-use claim or a historical-primary feature.
"""
import argparse
from collections import Counter
import csv
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

import collect_research_v1 as reference

KIND='cross_tool_alias_rescan_v1'
HUMAN=['human_relevance_label','reviewer','reviewed_at','review_notes']
REGENERATED={'collection_manifest.json','tool_paper_v1.jsonl','research_review_queue.csv',
             'research_review_sample.csv','research_v1_weekly.csv','research_v1_quality.json'}


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):value.update(block)
    return value.hexdigest()


def read(path):return json.loads(Path(path).read_text())


def safe(root,name):
    if not isinstance(name,str) or Path(name).is_absolute() or '..' in Path(name).parts:
        raise ValueError('Research artifact path must stay relative to its source')
    path=(root/name).resolve()
    if not path.is_relative_to(root) or not path.is_file():raise ValueError('Research artifact path missing/escaped')
    return path


def check_source(source,registry):
    manifest=read(source/'collection_manifest.json');cohort=digest(registry)
    if (manifest.get('source')!=reference.SOURCE or manifest.get('status')!='complete'
            or manifest.get('registry_sha256')!=cohort or digest(source/'tools.study.json')!=cohort):
        raise ValueError('A complete same-cohort research parent is required')
    for name,expected in manifest['files'].items():
        if digest(safe(source,name))!=expected:raise ValueError('Research parent file hash differs: '+name)
    if digest(source/'candidates_input.csv')!=manifest['candidate_input_sha256']:
        raise ValueError('Original candidate-query cohort changed')
    for request in manifest.get('requests',[]):
        path=safe(source,request['raw_path'])
        if digest(path)!=request['compressed_sha256']:raise ValueError('Research parent compressed raw hash differs')
        with gzip.open(path,'rb') as stream:body=stream.read(reference.MAX_BODY+1)
        if len(body)>reference.MAX_BODY or hashlib.sha256(body).hexdigest()!=request['raw_sha256']:
            raise ValueError('Research parent decoded raw hash/size differs')
    return manifest


def rows(path):
    with Path(path).open() as stream:return [json.loads(line) for line in stream if line.strip()]


def evidence(tool,paper):
    hits=[]
    for alias in dict.fromkeys(tool['research_exact_aliases']):
        pattern=re.compile(r'(?<!\w)'+re.escape(alias)+r'(?!\w)',re.I)
        for field in ['title_v1','abstract_v1']:
            text=paper[field]
            for match in pattern.finditer(text):
                hits.append({'alias':alias,'field':field,'start':match.start(),'end':match.end(),
                    'matched_text':match.group(),'context':text[max(0,match.start()-80):min(len(text),match.end()+80)]})
    return hits


def additions(papers,existing,registry,parent_sha,lag,derived_at,start,end):
    tools=registry['tools'];ids=[tool['tool_id'] for tool in tools]
    if not tools or len(set(ids))!=len(ids) or any(not tool.get('research_exact_aliases') for tool in tools):
        raise ValueError('Every frozen tool requires its complete alias set')
    if any(not isinstance(alias,str) or not alias.strip() for tool in tools for alias in tool['research_exact_aliases']):
        raise ValueError('Alias strings must be nonempty')
    known={(item['tool_id'],item['paper_id']) for item in existing}
    if len(known)!=len(existing) or {tid for tid,_ in known}-set(ids):raise ValueError('Duplicate/foreign existing tool-paper key')
    result=[]
    for paper in sorted(papers,key=lambda item:item['paper_id']):
        day=reference.stamp(paper['published']).date()
        if not date.fromisoformat(start)<=day<date.fromisoformat(end):continue
        for tool in sorted(tools,key=lambda item:item['tool_id']):
            if (tool['tool_id'],paper['paper_id']) in known:continue
            match=reference.classify(tool,paper,lag)
            if not match['strict_lexical_candidate']:continue
            proof=evidence(tool,paper)
            if not proof:raise ValueError('Classifier hit lacks exact first-version text evidence')
            item={'tool_id':tool['tool_id'],'paper_id':paper['paper_id'],'source':reference.SOURCE,
                'candidate_published':'','candidate_updated':'','candidate_observed_at':derived_at,
                'candidate_query':'offline_all_frozen_tool_aliases_over_retained_v1',
                'discovery_source':KIND,'v1_retrieval_status':'retrieved',
                'title_v1':paper['title_v1'],'abstract_v1':paper['abstract_v1'],
                'published':paper['published'],'v1_updated':paper['v1_updated'],
                'metadata_observed_at':paper['metadata_observed_at'],**match,
                'derived_at':derived_at,'derivation_policy':KIND,'parent_manifest_sha256':parent_sha,
                'parent_paper_record_sha256':hashlib.sha256(encoded(paper).encode()).hexdigest(),
                'alias_evidence':proof,'evidence_offset_unit':'Unicode code points within the named retained text field'}
            result.append(item);known.add((item['tool_id'],item['paper_id']))
    return sorted(result,key=lambda item:(item['tool_id'],item['paper_id']))


def csv_rows(path):
    with Path(path).open(newline='') as stream:
        reader=csv.DictReader(stream);return reader.fieldnames,list(reader)


def write_csv(path,fields,records,human=None):
    human=human or {}
    with path.open('x',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore');writer.writeheader()
        for row in records:
            row={**row,**human.get((row['tool_id'],row['paper_id']),{})}
            value={key:('; '.join(row.get(key,[])) if key=='matched_aliases' else
                        encoded(row[key]) if isinstance(row.get(key),(list,dict)) else row.get(key,'')) for key in fields}
            writer.writerow(value)


def expand_source(source,registry,research_root,output_root=None):
    source=Path(source).resolve();registry=Path(registry).resolve();research_root=Path(research_root).resolve()
    if not source.is_relative_to(research_root):raise ValueError('Research parent must be inside the declared research root')
    parent_sha=digest(source/'collection_manifest.json');parent=check_source(source,registry)
    classifier_sha=digest(Path(reference.__file__));code_sha=digest(Path(__file__));cohort=digest(registry)
    inputs={'kind':KIND,'parent_manifest_sha256':parent_sha,'registry_sha256':cohort,
            'code_sha256':code_sha,'classifier_code_sha256':classifier_sha}
    previous=parent.get('derivation',{})
    if (previous.get('kind')==KIND and previous.get('code_sha256')==code_sha
            and previous.get('classifier_code_sha256')==classifier_sha and previous.get('registry_sha256')==cohort
            and previous.get('all_retained_papers_all_tool_aliases_scanned') is True):return source
    fingerprint=hashlib.sha256(encoded(inputs).encode()).hexdigest()
    output_root=Path(output_root).resolve() if output_root else research_root/'derived_runs'
    if not output_root.is_relative_to(research_root):raise ValueError('Derived research output must stay inside the research root')
    if output_root.is_relative_to(source):raise ValueError('Derived output cannot be written inside its immutable parent')
    output=output_root/(parent['history_end_exclusive']+'-cross-tool-'+fingerprint[:16])
    if output.exists():
        child=check_source(output,registry)
        if (child.get('derived_from',{}).get('manifest_sha256')!=parent_sha
                or child.get('derivation',{}).get('input_fingerprint')!=fingerprint):
            raise ValueError('Existing research child is bound to different inputs')
        return output
    papers=rows(source/'papers_v1.jsonl');matched=rows(source/'tool_paper_v1.jsonl')
    paper_ids={paper['paper_id'] for paper in papers}
    if (len(paper_ids)!=len(papers) or len(papers)!=parent['retrieved_v1_papers']
            or len(papers)!=parent['unique_candidate_papers']
            or len(matched)!=parent['candidate_pairs'] or {item['paper_id'] for item in matched}!=paper_ids):
        raise ValueError('Parent complete paper/pair arithmetic differs')
    if any(item.get('historical_feature_eligible') is not False or item.get('manual_review_completed') is not False for item in matched):
        raise ValueError('This candidate-only derivation cannot rewrite an already promoted/reviewed source')
    for paper in papers:
        if (paper.get('requested_version')!=1 or paper.get('version_verified') is not True
                or not paper.get('returned_id','').endswith('v1')
                or reference.canonical_id(paper['returned_id'])!=paper['paper_id']):
            raise ValueError('Cross-tool evidence requires an explicitly verified first version')
    derived_at=reference.iso(datetime.now(timezone.utc))
    extra=additions(papers,matched,read(registry),parent_sha,parent['availability_lag_days'],derived_at,
                    parent['history_start'],parent['history_end_exclusive'])
    if not extra:return source
    all_rows=sorted(matched+extra,key=lambda item:(item['tool_id'],item['paper_id']))
    tools=sorted(tool['tool_id'] for tool in read(registry)['tools'])
    weekly=reference.weekly_rows(all_rows,tools,parent['history_start'],parent['history_end_exclusive'])
    weekly_fields,_=csv_rows(source/'research_v1_weekly.csv')
    if weekly and set(weekly_fields)!=set(weekly[0]):raise ValueError('Reference weekly research schema changed')
    queue_fields,old_queue=csv_rows(source/'research_review_queue.csv');_,old_sample=csv_rows(source/'research_review_sample.csv')
    human={(row['tool_id'],row['paper_id']):{field:row.get(field,'') for field in HUMAN} for row in old_queue+old_sample}
    queue_fields=list(dict.fromkeys(queue_fields+HUMAN+['discovery_source','derivation_policy','parent_manifest_sha256','derived_at','alias_evidence']))
    sample=reference.review_sample(all_rows)
    output_root.mkdir(parents=True,exist_ok=True)
    staging=Path(tempfile.mkdtemp(prefix='.cross-tool-',dir=output_root))
    try:
        for name in parent['files']:
            if name in REGENERATED:continue
            destination=staging/name;destination.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(safe(source,name),destination)
        lineage='lineage/collection_manifest-'+parent_sha+'.json'
        (staging/lineage).parent.mkdir(parents=True,exist_ok=True)
        (staging/lineage).write_bytes((source/'collection_manifest.json').read_bytes())
        with (staging/'tool_paper_v1.jsonl').open('x') as stream:
            for row in all_rows:stream.write(json.dumps(row,sort_keys=True,ensure_ascii=False,allow_nan=False)+'\n')
        added_name='cross_tool_added_pairs-'+fingerprint[:16]+'.jsonl'
        with (staging/added_name).open('x') as stream:
            for row in extra:stream.write(encoded(row)+'\n')
        write_csv(staging/'research_review_queue.csv',queue_fields,all_rows,human)
        write_csv(staging/'research_review_sample.csv',list(dict.fromkeys(queue_fields+['sample_stratum'])),sample,human)
        with (staging/'research_v1_weekly.csv').open('x',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=weekly_fields);writer.writeheader();writer.writerows(weekly)
        ancestry={'source_relative_path':str(source.relative_to(research_root)),'manifest_sha256':parent_sha,'manifest_copy_path':lineage}
        derivation={**inputs,'input_fingerprint':fingerprint,'candidate_only':True,
            'all_retained_papers_all_tool_aliases_scanned':True,'tools_scanned':len(tools),'papers_scanned':len(papers),
            'added_candidate_pairs':len(extra),'added_pairs_by_tool':dict(Counter(row['tool_id'] for row in extra)),
            'new_paper_ids':0,'provider_requests_added':0,'original_query_cohort_unchanged':True,
            'added_pairs_file':added_name}
        quality=read(source/'research_v1_quality.json')
        quality.update(status='complete',collection_complete=True,derived_at=derived_at,derived_from=ancestry,derivation=derivation,
            candidate_pairs=len(all_rows),unique_candidate_papers=len(papers),retrieved_v1_papers=len(papers),
            match_status_counts=dict(Counter(row['match_status'] for row in all_rows)),
            review_required_pairs=len(all_rows),weekly_rows=len(weekly),review_sample_rows=len(sample))
        quality['scope']='Retained first-version paper integrity and all-frozen-tool lexical candidate derivation; historical discovery recall remains uncertified.'
        quality['files']={str(path.relative_to(staging)):digest(path) for path in staging.rglob('*') if path.is_file()}
        (staging/'research_v1_quality.json').write_text(json.dumps(quality,indent=2,sort_keys=True)+'\n')
        child={**parent,'status':'complete','collection_complete':True,'derived_at':derived_at,'derived_from':ancestry,
            'derivation':derivation,'collector_resume_allowed':False,'candidate_pairs':len(all_rows),
            'seed_candidate_pairs':parent.get('seed_candidate_pairs',parent['candidate_pairs']),
            'files':{str(path.relative_to(staging)):digest(path) for path in staging.rglob('*') if path.is_file()}}
        (staging/'collection_manifest.json').write_text(json.dumps(child,indent=2,sort_keys=True)+'\n')
        check_source(staging,registry)
        if digest(source/'collection_manifest.json')!=parent_sha:raise ValueError('Parent research checkpoint changed during derivation')
        os.replace(staging,output)
    except BaseException:
        shutil.rmtree(staging);raise
    return output


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['source','registry','research-root']:parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--output-root',type=Path)
    args=parser.parse_args();output=expand_source(args.source,args.registry,args.research_root,args.output_root)
    manifest=read(output/'collection_manifest.json')
    print(json.dumps({'output':str(output),'manifest_sha256':digest(output/'collection_manifest.json'),
                     'derivation':manifest.get('derivation'),'candidate_pairs':manifest['candidate_pairs']}))


if __name__=='__main__':main()
