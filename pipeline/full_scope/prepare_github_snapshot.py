"""Freeze a GitHub archive state and prepare a checksum-verified rsync file list.

This makes no network requests and never edits the source collector state/raw.
The output is an immutable transfer bundle. Run its emitted rsync command first;
copy/activate the frozen state on the destination only after raw files arrive.
"""
import argparse
import copy
import hashlib
import gzip
import io
import re
import json
import os
from pathlib import Path
import shlex
import shutil
import tempfile
from datetime import datetime,timezone

ANCILLARY=['spacy_census_reconciliation.json','connector_rate_limit_receipts.jsonl','cursor_conformance.json',
           'updated_cursor_uint64_conformance.json','offline_hash_census_audit.json','spacy_no_since_probe_result.json']
PROOF_PREFIXES=('updated_','repo_identity_','proof_reference_','pull_','cursor_','spacy_','connector_','flax_','jsonp_','v3_','offline_','core_')


def sha(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
    return digest.hexdigest()


def evidence_graph(source_root,state,known_raw):
    """Freeze explicit transitive proof references without rewriting receipts."""
    files={};metadata={};proof_raw={};walking=set()
    def safe(relative):
        if not isinstance(relative,str) or '\\' in relative:raise ValueError('Invalid proof path')
        parts=Path(relative).parts
        if not parts or any(part in {'.','..'} for part in parts) or Path(relative).is_absolute():raise ValueError('Unsafe proof path')
        if parts[0]=='raw':
            allowed=relative.endswith('.json.gz') and all(re.fullmatch(r'[A-Za-z0-9._-]+',p) for p in parts)
        else:
            allowed=len(parts)==1 and ((relative.startswith(PROOF_PREFIXES) and re.fullmatch(r'[A-Za-z0-9._-]+\.json(?:l|\.gz)?',relative)) or relative=='sha256_v8.js')
        path=(source_root/relative).resolve()
        if not allowed or not path.is_relative_to(source_root):raise ValueError('Proof reference is outside the relative allowlist: '+relative)
        return path
    def digest(value):
        if value is None:return None
        if not isinstance(value,str) or not re.fullmatch(r'[a-f0-9]{64}',value):raise ValueError('Invalid proof SHA256')
        return value
    def hashes(obj,key,relative):
        base=key[:-5] if key.endswith('_path') else key
        prefixes=[base]
        if base.endswith('_raw'):prefixes.append(base[:-4])
        if base.endswith('_receipt_raw'):prefixes.append(base[:-12])
        if key=='path':prefixes.append('')
        compressed=[];raw=[];plain=[]
        for prefix in prefixes:
            def value(suffix):return obj.get(prefix+'_'+suffix if prefix else suffix)
            compressed.extend([value('compressed_sha256')]);raw.extend([value('raw_sha256')])
            plain.extend([value('sha256'),value('receipt_sha256')])
        if key=='raw_path':compressed.append(obj.get('compressed_sha256'));raw.append(obj.get('raw_sha256'))
        comp=next((digest(v) for v in compressed if v is not None),None)
        raw_hash=next((digest(v) for v in raw if v is not None),None)
        normal=next((digest(v) for v in plain if v is not None),None)
        return (comp if key=='raw_path' else comp or normal,raw_hash) if relative.endswith('.gz') else (normal or raw_hash,None)
    def load(relative,expected=None,raw_expected=None):
        path=safe(relative)
        if relative in known_raw:
            known_comp,known_decoded=known_raw[relative]['hashes']
            if expected and expected!=known_comp:raise ValueError('Proof conflicts with original raw receipt')
            if raw_expected and raw_expected!=known_decoded:raise ValueError('Proof conflicts with original decoded receipt')
            expected,raw_expected=known_comp,known_decoded
        expected=digest(expected);raw_expected=digest(raw_expected)
        if relative in metadata:
            info=metadata[relative]
            if expected and info['sha256']!=expected:raise ValueError('Conflicting proof checksum: '+relative)
            if raw_expected and info.get('decoded_sha256')!=raw_expected:raise ValueError('Conflicting decoded proof checksum: '+relative)
            info['producer_hash_bound'] |= expected is not None or raw_expected is not None
            return info.get('document')
        data=path.read_bytes();actual=hashlib.sha256(data).hexdigest()
        if expected and actual!=expected:raise ValueError('Changed referenced proof: '+relative)
        info={'sha256':actual,'producer_hash_bound':expected is not None or raw_expected is not None}
        if relative.endswith('.gz'):
            if expected is None and raw_expected is None:raise ValueError('Raw proof requires a producer checksum: '+relative)
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:decoded=stream.read(64*1024**2+1)
            if len(decoded)>64*1024**2:raise ValueError('Proof body exceeds the64MiB bound')
            decoded_hash=hashlib.sha256(decoded).hexdigest()
            if raw_expected and decoded_hash!=raw_expected:raise ValueError('Changed decoded proof: '+relative)
            info['decoded_sha256']=decoded_hash;info['document']=None
            if relative.startswith('raw/'):
                proof_raw[relative]={'hashes':(actual,decoded_hash),'bytes':len(data)}
            else:files[relative]=data
            metadata[relative]=info;return None
        if relative=='sha256_v8.js':
            if expected is None:raise ValueError('Hash implementation evidence requires its producer checksum')
            files[relative]=data;info['document']=None;metadata[relative]=info;return None
        try:
            document=[json.loads(line) for line in data.splitlines() if line.strip()] if relative.endswith('.jsonl') else json.loads(data)
        except (json.JSONDecodeError,UnicodeDecodeError) as error:
            raise ValueError('Proof/evidence is incomplete or changing; retry freeze: '+relative) from error
        files[relative]=data;info['document']=document;metadata[relative]=info
        if relative not in walking:
            walking.add(relative);visit(document,True)
        return document
    def visit(value,proof_document=False):
        if isinstance(value,list):
            for item in value:visit(item,proof_document)
        elif isinstance(value,dict):
            # These provider page hashes are deliberately stored in a separate
            # receipt; verify that receipt before following its gzip body.
            for page_key,receipt_key in [('provider_first_page_path','provider_receipt_path'),('tie_provider_page_path','tie_provider_receipt_path'),('provider_page_path','provider_receipt_path')]:
                if value.get(page_key) and value.get(receipt_key):
                    receipt=load(value[receipt_key],value.get(receipt_key[:-5]+'_sha256'))
                    load(value[page_key],receipt.get('compressed_sha256'),receipt.get('raw_sha256'))
            for key,item in value.items():
                if isinstance(item,str) and (re.search(r'\.json(?:l|\.gz)?$',item) or item.endswith('.js')):
                    reference=key in {'path','proof_path','protocol_receipt'} or key.endswith('_path')
                    if reference and (proof_document or key!='raw_path'):
                        expected,raw_expected=hashes(value,key,item);load(item,expected,raw_expected)
                elif isinstance(item,(dict,list)):visit(item,proof_document)
    # A producer-signed correction maps an incorrect old reference to the
    # correct immutable proof. Preserve original state/correction bytes and use
    # the correction only as a checked read-time traversal overlay.
    overlay=state;correction=state.get('proof_reference_corrections');correction_rows=[]
    if correction:
        path=safe(correction['path']);data=path.read_bytes();expected=digest(correction['sha256'])
        actual=hashlib.sha256(data).hexdigest()
        if actual!=expected:raise ValueError('Changed proof-reference correction artifact')
        document=json.loads(data);overlay=copy.deepcopy(state)
        original_requests=state['requests'];corrected_by_raw={}
        for entry in document.get('corrections',[]):
            relative=entry['raw_path'];known=known_raw.get(relative)
            if known is None or known['hashes'][1]!=entry['raw_sha256']:raise ValueError('Correction is not bound to an original raw receipt')
            matching=[i for i,r in enumerate(original_requests) if r.get('raw_path')==relative
                and r.get('raw_sha256')==entry['raw_sha256'] and r.get('tool_id')==entry['tool_id']
                and r.get('page')==entry['page'] and r.get('observed_at')==entry['original_observed_at']]
            if not matching:raise ValueError('Correction original observation is missing')
            for index in matching:
                row=overlay['requests'][index]
                original=(row.get('cursor_conformance_path'),row.get('cursor_conformance_receipt_sha256'))
                if original not in {(entry['original_path'],entry['original_sha256']),
                                    (entry['corrected_path'],entry['corrected_sha256'])}:
                    raise ValueError('Correction differs from original reference metadata')
                row['cursor_conformance_path']=entry['corrected_path']
                row['cursor_conformance_receipt_sha256']=entry['corrected_sha256']
            corrected_by_raw[relative]=entry;correction_rows.append(entry)
        for tool in overlay.get('tools',{}).values():
            visible=tool.get('issues_visible',{});entry=corrected_by_raw.get(visible.get('last_receipt_raw_path'))
            current=(visible.get('cursor_conformance_path'),visible.get('cursor_conformance_receipt_sha256'))
            if entry and current in {(entry['original_path'],entry['original_sha256']),
                                     (entry['corrected_path'],entry['corrected_sha256'])}:
                visible['cursor_conformance_path']=entry['corrected_path']
                visible['cursor_conformance_receipt_sha256']=entry['corrected_sha256']
        files[correction['path']]=data
        metadata[correction['path']]={'sha256':actual,'producer_hash_bound':True,'document':document,
            'correction_read_overlay_only':True,'correction_receipts_verified':len(correction_rows)}
        for entry in correction_rows:
            load(entry['original_path'])
            load(entry['corrected_path'],entry['corrected_sha256'])
            load(entry['raw_path'],known_raw[entry['raw_path']]['hashes'][0],entry['raw_sha256'])
    visit(overlay)
    for name in ANCILLARY:
        if (source_root/name).exists():load(name)
    proof_metadata={name:{k:v for k,v in info.items() if k!='document'} for name,info in metadata.items()}
    return files,proof_raw,proof_metadata


def prepare(source_root,state_path,registry_path,output_root,remote=None,ssh_key=None):
    source_root=Path(source_root).resolve();state_path=Path(state_path);registry_path=Path(registry_path)
    payload=state_path.read_bytes();state=json.loads(payload);fingerprint=hashlib.sha256(payload).hexdigest()
    if state.get('source')!='github_direct_issues_pulls' or state.get('registry_sha256')!=sha(registry_path):
        raise ValueError('Source type or frozen registry hash differs')
    captured=datetime.now(timezone.utc).isoformat()
    paths={}
    for request in state['requests']:
        relative=request['raw_path'];path=(source_root/relative).resolve()
        if (not path.is_relative_to(source_root) or not relative.startswith('raw/') or not relative.endswith('.json.gz')
                or relative.startswith('/') or '\n' in relative or '\r' in relative):
            raise ValueError('Unsafe relative archive path')
        value=(request['compressed_sha256'],request['raw_sha256'])
        if relative in paths:
            if paths[relative]['hashes']!=value:raise ValueError('Conflicting immutable response hashes')
            continue
        if not path.is_file() or sha(path)!=value[0]:raise ValueError('Missing or changed immutable response: '+relative)
        paths[relative]={'hashes':value,'bytes':path.stat().st_size}
    ancillary,proof_raw,proof_metadata=evidence_graph(source_root,state,paths)
    for relative,info in proof_raw.items():
        if relative in paths and paths[relative]['hashes']!=info['hashes']:raise ValueError('Conflicting transitive raw proof')
        paths[relative]=info
    ancillary_hashes={name:hashlib.sha256(data).hexdigest() for name,data in ancillary.items()}
    lineage=hashlib.sha256(json.dumps({'state_sha256':fingerprint,'ancillary':ancillary_hashes,
        'proof_raw':{name:value['hashes'] for name,value in proof_raw.items()}},sort_keys=True).encode()).hexdigest()
    output_root=Path(output_root);output_root.mkdir(parents=True,exist_ok=True)
    output=output_root/('snapshot-'+(fingerprint[:12]+'-'+lineage[:12] if ancillary else fingerprint[:16]))
    state_name='collection_state.'+fingerprint[:12]+'.json'
    if output.exists():
        if (output/state_name).read_bytes()!=payload or sha(output/'tools.study.json')!=state['registry_sha256']:
            raise ValueError('Existing immutable transfer bundle differs')
        manifest=json.loads((output/'transfer_manifest.json').read_text())
        for relative,digest in manifest['files'].items():
            path=(output/relative).resolve()
            if not path.is_relative_to(output.resolve()) or sha(path)!=digest:raise ValueError('Changed immutable transfer bundle')
        return manifest
    staging=Path(tempfile.mkdtemp(prefix='.github-transfer-',dir=output_root))
    try:
        (staging/state_name).write_bytes(payload);(staging/'tools.study.json').write_bytes(registry_path.read_bytes())
        (staging/'raw_files.txt').write_text(''.join(relative+'\n' for relative in sorted(paths)))
        ancillary_files={}
        for name,data in ancillary.items():
            # Preserve source-relative names for protocol resolvers and retain
            # an independent immutable lineage copy for historical receipts.
            for relative,role in [(name,'source_relative'),('lineage/'+lineage[:16]+'/'+name,'lineage_archive')]:
                path=staging/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
                ancillary_files[relative]={'source_name':name,'role':role,'sha256':ancillary_hashes[name],
                    'bytes':len(data),'captured_at':captured}
        (staging/'ancillary_files.txt').write_text(''.join(name+'\n' for name in sorted(ancillary_files)))
        command=['rsync','-a','--checksum','--files-from='+str((output/'raw_files.txt').resolve()),'--stats']
        if ssh_key:command.extend(['-e',shlex.join(['ssh','-i',str(Path(ssh_key).expanduser()),'-o','BatchMode=yes'])])
        command.extend([str(source_root)+'/',remote.rstrip('/')+'/' if remote else '<ssh-host>:/absolute/archive/root/'])
        ancillary_command=[value.replace('raw_files.txt','ancillary_files.txt') for value in command[:-2]]
        ancillary_command.extend([str(output.resolve())+'/',command[-1]])
        manifest={'preparation_format_version':3,'preparation_code_sha256':sha(Path(__file__)),
            'state_sha256':fingerprint,'registry_sha256':state['registry_sha256'],
            'source_root':str(source_root),'source_state':str(state_path.resolve()),'bundle':str(output.resolve()),
            'frozen_state_name':state_name,'requests':len(state['requests']),'raw_files':len(paths),
            'compressed_bytes':sum(v['bytes'] for v in paths.values()),
            'files':{name:sha(staging/name) for name in [state_name,'tools.study.json','raw_files.txt','ancillary_files.txt',*ancillary_files]},
            'lineage_sha256':lineage,'ancillary_files':ancillary_files,'proof_evidence':proof_metadata,
            'transitive_proof_raw_files':sorted(proof_raw),
            'optional_ancillary_absent':[name for name in ANCILLARY if name not in ancillary],
            'rsync_argv':command,'rsync_command':shlex.join(command),
            'ancillary_rsync_argv':ancillary_command if ancillary_files else [],
            'ancillary_rsync_command':shlex.join(ancillary_command) if ancillary_files else None,
            'activation_rule':'Transfer raw files and frozen ancillary_files first. Upload registry and transfer_manifest.json, then state as .pending; verify remote raw/ancillary checksums and cohort, then atomically rename state last. Build a fresh feature run and gate its canonical pointer through inspect_source.'}
        (staging/'transfer_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        os.replace(staging,output)
    except BaseException:shutil.rmtree(staging,ignore_errors=True);raise
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root',type=Path,required=True);parser.add_argument('--state',type=Path)
    parser.add_argument('--registry',type=Path);parser.add_argument('--output-root',type=Path,required=True)
    parser.add_argument('--remote');parser.add_argument('--ssh-key',type=Path)
    args=parser.parse_args()
    result=prepare(args.source_root,args.state or args.source_root/'collection_state.json',
        args.registry or args.source_root/'tools.study.json',args.output_root,args.remote,args.ssh_key)
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
