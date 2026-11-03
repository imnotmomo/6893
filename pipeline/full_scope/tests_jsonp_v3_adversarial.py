"""Offline adversarial checks for actual provider pagination links."""
import copy
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

import github_jsonp_lists as subject

TOOL={'tool_id':'flax','github_repo':'google/flax','github_repo_id':233016299}
SINCE='2022-12-31T23:59:59Z'
COHORT='fixture-cohort'
BASE='https://api.github.com/repos/google/flax/issues'
PARAMS={'state':'all','sort':'updated','direction':'asc','since':SINCE,'per_page':'100','callback':'radarPagination'}
FIRST=BASE+'?'+urlencode(PARAMS)
PROVIDER='https://api.github.com/repositories/233016299/issues?'+urlencode({**PARAMS,'after':'opaque-provider-position','page':'2'})
NEXT=subject.connector_url(PROVIDER,TOOL,SINCE)
utc=lambda value:datetime.fromisoformat(value.replace('Z','+00:00'))
digest=lambda body:hashlib.sha256(body).hexdigest()


class ProviderLinks(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.serial=0

    def receipt(self,data=None,meta=None,url=FIRST,page=1):
        data=[{'id':1,'number':1,'created_at':'2023-01-01T00:00:00Z','updated_at':'2024-01-01T00:00:00Z'}] if data is None else data
        meta={'status':200,'link':[[PROVIDER,{'rel':'next'}]],'etag':'fixture'} if meta is None else meta
        payload=json.dumps(data,separators=(',',':'),ensure_ascii=False).encode()
        prefix=b'{"meta":'+json.dumps(meta,separators=(',',':')).encode()+b',"data":'
        raw=prefix+payload+b'}';path=self.root/f'page-{self.serial}.json.gz';self.serial+=1
        path.write_bytes(gzip.compress(raw,mtime=0))
        next_links=[v[0] for v in meta.get('link',[]) if v[1].get('rel')=='next']
        provider=next_links[0] if len(next_links)==1 else None
        observed=f'2026-11-03T05:{page:02d}:30Z'
        return {'transport':'github_connector_jsonp_envelope','registry_sha256':COHORT,
            'raw_path':path.name,'compressed_sha256':digest(path.read_bytes()),'raw_sha256':digest(raw),
            'data_json_utf8_offset':len(prefix),'data_json_utf8_bytes':len(payload),'data_sha256':digest(payload),
            'full_envelope_sha256':digest(b'original envelope'),'full_envelope_bytes':len(raw)+100,
            'status':200,'record_count':len(data),'provider_next_url':provider,
            'connector_next_url':subject.connector_url(provider,TOOL,SINCE) if provider else None,
            'phase':'issues_list_v3','stream':'issues_visible_v3','kind':'issues','tool_id':'flax','page':page,
            'since':SINCE,'url':url,'request_url':url,'terminal_page':provider is None,
            'request_started_at':f'2026-11-03T05:{page:02d}:00Z','observed_at':observed}

    def evaluate(self,rows):
        last=rows[-1]
        source={'status':'complete','pages_saved':len(rows),'record_observations':sum(r['record_count'] for r in rows),
            'next_page':last['page']+1,'last_receipt_raw_path':last['raw_path'],'terminal_raw_path':last['raw_path']}
        state={'registry_sha256':COHORT,'start':'2023-01-01T00:00:00Z','requests':rows,
            'tools':{'flax':{'issues_visible_v3':source}}}
        def normalize(row,tool,receipt):
            return {'tool_id':tool['tool_id'],'kind':'issue','number':row['number'],'created_at':row['created_at'],
                'merged_at_field_present':True,'closed_at_field_present':True}
        with patch.object(subject,'verify_receipts',return_value={}):
            return subject.evaluate_v3(self.root,state,{'tools':[TOOL]},normalize,utc,1024**2)

    def test_opaque_provider_link_and_nonempty_terminal_are_authoritative(self):
        first=self.receipt();last=self.receipt(meta={'status':200},url=NEXT,page=2,
            data=[{'id':2,'number':2,'created_at':'2023-01-01T00:00:00Z','updated_at':'2024-01-02T00:00:00Z'}])
        observations,quality,_=self.evaluate([first,last])
        self.assertEqual(len(observations),2)
        self.assertTrue(quality['flax']['visible_list_pagination_complete'])

    def test_unicode_and_code_looking_text_remain_exact_data(self):
        data=[{'id':1,'title':'模型 🧠 café é','body':'}); throw new Error("data only"); /*'}]
        receipt=self.receipt(data=data,meta={'status':200})
        restored,nxt=subject.envelope(self.root,receipt,COHORT,1024**2)
        self.assertEqual(restored,data);self.assertIsNone(nxt)
        changed={**receipt,'data_json_utf8_bytes':receipt['data_json_utf8_bytes']-1}
        with self.assertRaises(ValueError):subject.envelope(self.root,changed,COHORT,1024**2)

    def test_reported_updated_at_does_not_replace_the_opaque_link(self):
        data=[{'id':8,'number':8,'created_at':'2023-01-01T00:00:00Z','updated_at':'2026-04-06T17:02:41Z'},
              {'id':7,'number':7,'created_at':'2023-01-01T00:00:00Z','updated_at':'2024-02-24T03:15:02Z'}]
        first=self.receipt(data=data);last=self.receipt(data=[],meta={'status':200},url=NEXT,page=2)
        _,quality,_=self.evaluate([first,last])
        self.assertTrue(quality['flax']['visible_list_pagination_complete'])

    def test_an_invented_next_cursor_cannot_certify_a_chain(self):
        first=self.receipt();last=self.receipt(data=[],meta={'status':200},url=NEXT.replace('opaque-provider-position','invented'),page=2)
        _,quality,_=self.evaluate([first,last])
        self.assertFalse(quality['flax']['visible_list_pagination_complete'])
        self.assertIn('v3_actual_link_chain_differs',quality['flax']['reasons'])

    def test_next_link_and_retained_payload_tampering_fail_closed(self):
        receipt=self.receipt();receipt['provider_next_url']=None
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)
        receipt=self.receipt();receipt['data_sha256']='0'*64
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)
        receipt=self.receipt();(self.root/receipt['raw_path']).write_bytes(b'changed')
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)

    def test_authentication_metadata_is_not_an_archive_field(self):
        receipt=self.receipt(meta={'status':200,'x-oauth-client-id':'must-not-be-retained'})
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)

    def test_duplicate_next_relations_are_rejected(self):
        receipt=self.receipt(meta={'status':200,'link':[[PROVIDER,{'rel':'next'}],[PROVIDER,{'rel':'next'}]]})
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)

    def test_cohort_path_size_and_slice_bounds_are_checked(self):
        for key,value in [('registry_sha256','foreign'),('raw_path','../escape.json.gz'),
            ('data_json_utf8_offset',False),('data_json_utf8_bytes',-1),('full_envelope_sha256','missing')]:
            r=self.receipt();r[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):subject.envelope(self.root,r,COHORT,1024**2)
        r=self.receipt()
        with self.assertRaises(ValueError):subject.envelope(self.root,r,COHORT,10)

    def test_only_the_numeric_repository_path_alias_may_change(self):
        self.assertEqual(NEXT,PROVIDER.replace('/repositories/233016299/','/repos/google/flax/'))
        for changed in [PROVIDER.replace('233016299','233016298'),PROVIDER.replace('api.github.com','example.org'),
            PROVIDER.replace('state=all','state=open'),PROVIDER+'&since=changed',PROVIDER+'&access_token=bad',
            PROVIDER.replace('callback=radarPagination','callback=other')]:
            with self.subTest(url=changed),self.assertRaises(ValueError):subject.connector_url(changed,TOOL,SINCE)

    def test_ambiguous_casefolded_status_metadata_is_rejected(self):
        receipt=self.receipt(meta={'Status':403,'status':200})
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)

    def test_boolean_record_count_is_not_an_integer_count(self):
        receipt=self.receipt();receipt['record_count']=True
        with self.assertRaises(ValueError):subject.envelope(self.root,receipt,COHORT,1024**2)


if __name__=='__main__':unittest.main()
