import unittest
import copy
import json
import hashlib
from pathlib import Path
import tempfile
from unittest import mock
from datetime import date, timedelta
from run_download_backfill import uncovered_windows,compatible_receipt,split_windows,archive_inventory,verified_download_output,_BODY_HASH_CACHE
from pypi_rich_plan import plan


class DispatchIntervals(unittest.TestCase):
    def days(self, windows):
        return [a+timedelta(days=i) for a,b in windows for i in range((b-a).days)]

    def test_full_history_exactly_once(self):
        a,b=date(2023,1,1),date(2026,11,3)
        windows=list(uncovered_windows(a,b,set()))
        self.assertEqual(self.days(windows),[a+timedelta(days=i) for i in range((b-a).days)])
        self.assertEqual(windows[0],(a,date(2023,1,2)))
        self.assertEqual(windows[-1],(date(2026,11,2),b))
        self.assertTrue(all((y-x).days<=7 for x,y in windows))

    def test_completed_pilot_is_reused(self):
        a,b=date(2026,10,19),date(2026,11,3)
        covered={date(2026,10,26)+timedelta(days=i) for i in range(7)}
        days=self.days(list(uncovered_windows(a,b,covered)))
        self.assertFalse(set(days)&covered)
        self.assertEqual(len(days),8)
        self.assertEqual(len(days),len(set(days)))

    def test_completed_middle_day_splits_week(self):
        a,b=date(2026,10,19),date(2026,10,26)
        covered={date(2026,10,21)}
        self.assertEqual(list(uncovered_windows(a,b,covered)),[(a,date(2026,10,21)),(date(2026,10,22),b)])

    def test_partial_current_week_uses_daily_children(self):
        a,b=date(2026,11,2),date(2026,11,6)
        self.assertEqual([int((y-x).days) for x,y in uncovered_windows(a,b,set())],[1,1,1,1])

    def test_completed_daily_children_never_bought_again(self):
        a,b=date(2026,11,2),date(2026,11,9)
        covered={a+timedelta(days=i) for i in range(4)}
        self.assertEqual(list(uncovered_windows(a,b,covered)),[(date(2026,11,6),b)])

    def test_no_future_or_empty_intervals(self):
        a=date(2027,1,1)
        self.assertEqual(list(uncovered_windows(a,a,set())),[])
        self.assertEqual(list(uncovered_windows(a,a-timedelta(days=1),set())),[])

    def test_foreign_or_incomplete_export_cannot_suppress_needed_queries(self):
        registry={'tools':[{'tool_id':'torch','pypi_package':'torch'}]}
        definition=plan(registry,'2026-11-01','2026-11-02')
        value={'status':'complete','source':'pypi_download_dimensions','source_plan':definition,
            'query_sha256':definition['query_sha256'],'job_id':'same-job','total_rows':1,'rows_fetched':1,
            'all_pages_fetched':True,'job_complete':True,'last_page_fetched':True,
            'decoded_records_sha256':'retained-body-sha'}
        receipt={'all_pages_fetched':True,'job_complete':True,'total_rows':1,
            'query_sha256':definition['query_sha256'],'cloud_job_id':'same-job','output_sha256':'retained-body-sha'}
        self.assertTrue(compatible_receipt(value,receipt,registry,True))
        self.assertFalse(compatible_receipt(value,receipt,registry,False))
        for field,bad in [('query_sha256','other'),('total_rows',2),('cloud_job_id','other'),
                          ('output_sha256','other'),('job_complete',False),('all_pages_fetched',False)]:
            changed={**receipt,field:bad}
            with self.subTest(field=field):self.assertFalse(compatible_receipt(value,changed,registry,True))
        foreign=copy.deepcopy(value);foreign['source_plan']['package_to_tool']={'different':'different'}
        self.assertFalse(compatible_receipt(foreign,receipt,registry,True))
        for field in ['all_pages_fetched','job_complete','last_page_fetched']:
            self.assertFalse(compatible_receipt({**value,field:False},receipt,registry,True))
        self.assertFalse(compatible_receipt(value,{**receipt,'total_rows':True},registry,True))

    def test_oversized_windows_split_without_gaps_or_future_days(self):
        start=date(2026,10,19)
        for days in range(2,15):
            end=start+timedelta(days=days);children=split_windows(start,end)
            self.assertEqual(self.days(children),self.days([(start,end)]))
            self.assertTrue(all(a<b and b<=end and (b-a).days<days for a,b in children))
        self.assertEqual(split_windows(start,start+timedelta(days=1)),[])


class PendingQueryReuse(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.registry={'tools':[{'tool_id':'torch','pypi_package':'torch'}]}
        self.start=date(2026,8,17);self.end=date(2026,8,24)

    def record(self,start,end,status):
        definition=plan(self.registry,str(start),str(end));folder=self.root/(str(start)+'-'+str(end));folder.mkdir()
        value={'source':'pypi_download_dimensions','status':status,'source_plan':definition,
            'query_sha256':definition['query_sha256'],'rows_fetched':1,'total_rows':1,'job_id':'job-'+folder.name,
            'decoded_records_sha256':hashlib.sha256(b'fixture data').hexdigest(),'job_complete':True,'all_pages_fetched':True,'last_page_fetched':True}
        (folder/'manifest.json').write_text(json.dumps(value));(folder/'query_plan.json').write_text(json.dumps(definition))
        if status=='complete':
            (folder/'decoded_records.jsonl.gz').write_bytes(b'fixture data')
            (folder/'completion_receipt.json').write_text(json.dumps({'query_sha256':definition['query_sha256'],
                'total_rows':1,'cloud_job_id':value['job_id'],'output_sha256':value['decoded_records_sha256'],
                'all_pages_fetched':True,'job_complete':True}))
        return folder

    def test_split_tree_remains_intact_after_a_prefix_finishes(self):
        self.record(self.start,self.end,'split')
        self.record(self.start,date(2026,8,20),'split')
        self.record(self.start,date(2026,8,18),'complete')
        self.record(date(2026,8,18),date(2026,8,20),'fetching')
        self.record(date(2026,8,20),self.end,'planned')
        _,covered,pending,blocked=archive_inventory(self.root,self.registry,self.start,self.end)
        self.assertEqual(covered,{self.start})
        self.assertEqual([(t['start'],t['end']) for t in pending],[(self.start,self.end)])
        self.assertEqual(list(uncovered_windows(self.start,self.end,blocked)),[])

    def test_charged_partial_interval_is_resumed_without_a_larger_overlap(self):
        self.record(self.start,date(2026,8,18),'complete')
        self.record(date(2026,8,18),date(2026,8,20),'fetching')
        _,_,pending,blocked=archive_inventory(self.root,self.registry,self.start,self.end)
        self.assertEqual([(t['start'],t['end']) for t in pending],[(date(2026,8,18),date(2026,8,20))])
        self.assertEqual(list(uncovered_windows(self.start,self.end,blocked)),[(date(2026,8,20),self.end)])

    def test_complete_children_cover_the_period_without_rebuying_the_parent(self):
        self.record(self.start,self.end,'split')
        self.record(self.start,date(2026,8,20),'complete')
        self.record(date(2026,8,20),self.end,'complete')
        _,covered,pending,blocked=archive_inventory(self.root,self.registry,self.start,self.end)
        self.assertEqual(len(covered),7);self.assertEqual(pending,[])
        self.assertEqual(list(uncovered_windows(self.start,self.end,blocked)),[])

    def test_corrupt_body_requeues_its_existing_interval_instead_of_claiming_coverage(self):
        folder=self.record(self.start,self.end,'complete')
        (folder/'decoded_records.jsonl.gz').write_bytes(b'corrupted body')
        _,covered,pending,blocked=archive_inventory(self.root,self.registry,self.start,self.end)
        self.assertEqual(covered,set());self.assertEqual(len(pending),1)
        self.assertEqual(pending[0]['output'],folder)
        self.assertEqual(list(uncovered_windows(self.start,self.end,blocked)),[])

    def test_changed_body_between_inventories_invalidates_cached_hash(self):
        folder=self.record(self.start,self.end,'complete');body=folder/'decoded_records.jsonl.gz'
        _,covered,_,_=archive_inventory(self.root,self.registry,self.start,self.end)
        self.assertEqual(len(covered),7)
        # Same byte length and restored mtime still changes ctime; reuse must
        # bind every requested identity field rather than just path/size/mtime.
        import os
        previous=body.stat();body.write_bytes(b'changed data')
        os.utime(body,ns=(previous.st_atime_ns,previous.st_mtime_ns))
        _,covered,pending,_=archive_inventory(self.root,self.registry,self.start,self.end)
        self.assertEqual(covered,set());self.assertEqual(len(pending),1)

    def test_unchanged_second_inventory_reuses_digest_without_reopening_body(self):
        folder=self.record(self.start,self.end,'complete');body=folder/'decoded_records.jsonl.gz'
        digest=hashlib.sha256(body.read_bytes()).hexdigest()
        self.assertTrue(verified_download_output(body,digest))
        with mock.patch.object(Path,'open',side_effect=AssertionError('cached body reopened')):
            self.assertTrue(verified_download_output(body,digest))

    def test_foreign_or_incomplete_metadata_never_hashes_body(self):
        folder=self.record(self.start,self.end,'complete');path=folder/'manifest.json'
        original=json.loads(path.read_text())
        foreign=copy.deepcopy(original);foreign['source_plan']['package_to_tool']={'foreign':'foreign'}
        path.write_text(json.dumps(foreign))
        with mock.patch('run_download_backfill.verified_download_output',side_effect=AssertionError('foreign body read')):
            self.assertEqual(archive_inventory(self.root,self.registry,self.start,self.end)[1],set())
        path.write_text(json.dumps(original));receipt=folder/'completion_receipt.json'
        value=json.loads(receipt.read_text());value['job_complete']=False;receipt.write_text(json.dumps(value))
        with mock.patch('run_download_backfill.verified_download_output',side_effect=AssertionError('incomplete body read')):
            self.assertEqual(archive_inventory(self.root,self.registry,self.start,self.end)[1],set())

    def test_file_change_during_hash_cannot_be_cached_as_complete(self):
        folder=self.record(self.start,self.end,'complete');body=folder/'decoded_records.jsonl.gz'
        expected=hashlib.sha256(body.read_bytes()).hexdigest();_BODY_HASH_CACHE.clear()
        factory=hashlib.sha256
        class ChangingHasher:
            def __init__(self):self.inner=factory()
            def update(self,block):self.inner.update(block)
            def hexdigest(self):
                body.write_bytes(b'file changed while hashing')
                return self.inner.hexdigest()
        with mock.patch('run_download_backfill.hashlib.sha256',side_effect=ChangingHasher):
            self.assertFalse(verified_download_output(body,expected))
        self.assertEqual(_BODY_HASH_CACHE,{})


if __name__=='__main__':unittest.main()
