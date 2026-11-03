#!/usr/bin/env python3
"""Archive public release JSON received as data over private stdin; no HTTP.

The same Archive helper is used by the bounded public collector. A successful
connector response does not expose raw HTTP headers; that limitation is recorded.
"""
from datetime import datetime,timezone
import argparse
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import tty
from urllib.parse import urlparse,parse_qs

SOURCE='github_repository_releases'
def digest(raw):return hashlib.sha256(raw).hexdigest()
def now():return datetime.now(timezone.utc).isoformat()
def atomic(path,value):
    temporary=path.with_suffix(path.suffix+'.tmp');temporary.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n');temporary.replace(path)


class Archive:
    def __init__(self,root,registry,end_exclusive,start='2023-01-01'):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
        self.registry_raw=Path(registry).read_bytes();self.registry=json.loads(self.registry_raw)
        self.tools={item['tool_id']:item for item in self.registry['tools']}
        if len(self.tools)!=30 or len(self.registry['tools'])!=30:raise ValueError('Frozen thirty-tool registry required')
        self.path=self.root/'collection_manifest.json'
        self.state=json.loads(self.path.read_text()) if self.path.exists() else {
            'source':SOURCE,'registry_sha256':digest(self.registry_raw),'history_start':start,
            'history_end_exclusive':end_exclusive,'as_of':now(),'started_at':now(),'status':'collecting',
            'requests':[],'tools':{},'historical_primary_eligible':False,'historical_visibility_complete':False,
            'pypi_release_correspondence_verified':False,
            'measurement':'Currently visible repository releases; published_at is the event date. created_at may be the tag-commit date.',
            'scope':'Repository-wide releases, not verified releases of the selected PyPI distribution.'}
        if (self.state.get('source')!=SOURCE or self.state.get('registry_sha256')!=digest(self.registry_raw)
                or self.state.get('history_start')!=start or self.state.get('history_end_exclusive')!=end_exclusive):
            raise ValueError('Release archive scope or cohort changed')
        copy=self.root/'tools.study.json'
        if copy.exists() and copy.read_bytes()!=self.registry_raw:raise ValueError('Frozen registry copy changed')
        if not copy.exists():copy.write_bytes(self.registry_raw)
        self.checkpoint()

    def checkpoint(self):self.state['updated_at']=now();atomic(self.path,self.state)

    def save(self,item):
        tid=item['tool_id'];kind=item['kind'];phase=item['phase'];page=int(item.get('page',1));size=int(item.get('per_page',100))
        if tid not in self.tools or kind not in {'repository','releases'} or phase not in {'identity','list','anchor','error'}:
            raise ValueError('Unexpected release archive scope')
        if page<1 or not 1<=size<=100:raise ValueError('Invalid release page bounds')
        url=urlparse(item['url']);tool=self.tools[tid]
        allowed={'/repos/'+tool['github_repo'],'/repositories/'+str(tool['github_repo_id'])}
        expected=allowed if kind=='repository' else {path+'/releases' for path in allowed}
        if url.scheme!='https' or url.netloc!='api.github.com' or url.path not in expected or url.fragment:
            raise ValueError('Release request leaves frozen repository scope')
        query=parse_qs(url.query)
        if kind=='releases' and (set(query)!={'per_page','page'} or query['per_page']!=[str(size)] or query['page']!=[str(page)]):
            raise ValueError('Release page query differs from archived cursor')
        body=item['body'];raw=body.encode('utf-8')
        if len(raw)>32*1024**2:raise ValueError('Release response exceeds archive size guard')
        try:parsed=json.loads(body)
        except json.JSONDecodeError:
            if phase!='error':raise
            parsed=None
        scope=digest(json.dumps({key:value for key,value in item.items() if key!='body'},sort_keys=True).encode())[:16]
        relative=Path('raw')/tid/(f"{len(self.state['requests'])+1:06d}-{phase}-{page:05d}-{scope}-{digest(raw)[:16]}.json.gz")
        target=self.root/relative;target.parent.mkdir(parents=True,exist_ok=True);compressed=gzip.compress(raw,mtime=0)
        if target.exists() and target.read_bytes()!=compressed:raise ValueError('Immutable release response collision')
        if not target.exists():target.write_bytes(compressed)
        meta={key:value for key,value in item.items() if key!='body'}
        meta.update(source=SOURCE,registry_sha256=digest(self.registry_raw),raw_path=str(relative),
            raw_sha256=digest(raw),compressed_sha256=digest(compressed),raw_bytes=len(raw),
            compressed_bytes=len(compressed),records=len(parsed) if isinstance(parsed,list) else None)
        sidecar=target.with_name(target.name+'.manifest.json')
        if not sidecar.exists():sidecar.write_text(json.dumps(meta,indent=2,sort_keys=True)+'\n')
        self.state['requests'].append(meta)
        return {'saved':str(relative),'records':meta['records'],'raw_bytes':len(raw)}


def main():
    os.umask(0o077);parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--end-exclusive',required=True);args=parser.parse_args()
    archive=Archive(args.root,args.registry,args.end_exclusive)
    if sys.stdin.isatty():tty.setraw(sys.stdin.fileno())
    print(json.dumps({'writer_ready':True,'root':str(archive.root)}),flush=True)
    for line in sys.stdin:
        try:
            payload=json.loads(line)
            if isinstance(payload,dict) and 'gzip_base64' in payload:
                payload=json.loads(gzip.decompress(base64.b64decode(payload['gzip_base64'],validate=True)))
            if isinstance(payload,dict) and payload.get('action')=='finish':
                archive.state['status']=payload.get('status','collected_unvalidated');archive.checkpoint()
                print(json.dumps({'writer_finished':True,'requests':len(archive.state['requests'])}),flush=True);return
            if isinstance(payload,dict) and payload.get('action')=='state':
                archive.state['tools'].update(payload.get('tools',{}));archive.checkpoint()
                print(json.dumps({'state_saved':True}),flush=True);continue
            items=payload if isinstance(payload,list) else [payload]
            saved=[archive.save(item) for item in items];archive.checkpoint()
            print(json.dumps({'saved_pages':len(saved),'records':sum(item['records'] or 0 for item in saved),
                              'total_requests':len(archive.state['requests'])}),flush=True)
        except Exception as error:print(json.dumps({'error':type(error).__name__+': '+str(error)}),flush=True)


if __name__=='__main__':main()
