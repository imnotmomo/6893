"""Adversarial native-prefix and shared-budget fixtures; no provider requests."""
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
from urllib.parse import parse_qs,urlsplit

import build_github_activity as b
import native_pr_updates as native
import dispatch_github_activity as dispatcher
import refresh_github_activity as r
from github_activity_overlay import body as decode_body
from tests_build_github_activity import pull

NOW=datetime(2023,1,23,12,tzinfo=timezone.utc)
REGISTRY={'tools':[{'tool_id':'tool','github_repo':'org/repo','github_repo_id':42}]}
TOOL=REGISTRY['tools'][0]


def row(number,updated='2023-01-20T00:00:00Z'):
    item=pull(number,'2022-01-01T00:00:00Z');item['updated_at']=updated;return item


def page_url(number):
    return r.initial_url(TOOL,'pulls','ignored')+f'&page={number}'


class NativeAdversarial(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.registry=self.root/'registry.json';self.registry.write_text(json.dumps(REGISTRY));self.cohort=b.sha(self.registry)
        self.baseline=self.root/'baseline';self.baseline.mkdir()
        (self.baseline/'manifest.json').write_text(json.dumps({'source':b.SOURCE,'registry_sha256':self.cohort,
            'history_end_exclusive':'2023-01-16T00:00:00Z'}))
        self.output=self.root/'updates';self.calls=[];self.pages={1:([row(1)],{})};self.repository={'id':42,'full_name':'org/repo'}

    def fetch(self,url):
        self.calls.append(url)
        if '/pulls?' in url:
            number=int(parse_qs(urlsplit(url).query).get('page',['1'])[0]);value,headers=self.pages[number]
        else:value,headers=self.repository,{}
        return {'status':200,'url':url,'body':json.dumps(value).encode(),'headers':headers}
    def step(self,**kwargs):return native.run(self.registry,self.output,self.baseline,1,0,self.fetch,NOW,**kwargs)
    def state(self):return json.loads((self.output/native.STATE).read_text())
    def decode(self,state=None):return native.decode_completed(self.output,state or self.state(),REGISTRY,self.cohort,decode_body)
    def assert_no_watermark(self):
        state=self.state();self.assertFalse(state['completed_cycles'])
        self.assertIsNone(state['streams']['tool']['last_success_end_exclusive'])

    def test_old_created_native_merge_is_independent_of_issue_wrappers(self):
        self.pages[1][0][0].update(merged_at='2023-01-19T00:00:00Z',closed_at='2023-01-19T00:00:00Z')
        self.step();rows,coverage,_=self.decode()
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['merged_at'],'2023-01-19T00:00:00Z')
        self.assertIn(('tool','pr_merge'),coverage)
        self.assertFalse(any('/issues' in url for url in self.calls))

    def test_events_after_native_update_never_advance_a_watermark(self):
        self.pages[1][0][0]['merged_at']='2023-01-21T00:00:00Z'
        result=self.step();self.assertTrue(result['errors']);self.assert_no_watermark()
        self.assertEqual(self.decode()[0],[])

    def test_descending_order_must_hold_across_actual_pages(self):
        self.pages={1:([row(n) for n in range(1,101)],{'Link':f'<{page_url(2)}>; rel="next"'}),
                    2:([row(101,'2023-01-21T00:00:00Z')],{})}
        self.step();self.assert_no_watermark();self.step();self.assert_no_watermark()
        self.assertTrue(self.state()['failed_cycles'])

    def test_full_strictly_old_page_can_end_prefix_but_equal_boundary_cannot(self):
        self.pages={1:([row(n,'2023-01-08T23:59:59Z') for n in range(1,101)],
                       {'Link':f'<{page_url(2)}>; rel="next"'})}
        self.step();state=self.state();self.assertEqual(len(state['completed_cycles']),1)
        self.assertEqual(state['completed_cycles'][0]['terminal_reason'],'verified_old_native_updated_boundary')
        rows,coverage,_=self.decode();self.assertEqual(rows,[]);self.assertIn(('tool','pr_creation'),coverage)

    def test_equal_boundary_or_short_old_page_does_not_prove_tail(self):
        self.pages={1:([row(n,'2023-01-09T00:00:00Z') for n in range(1,101)],{'Link':f'<{page_url(2)}>; rel="next"'})}
        self.step();self.assert_no_watermark()

    def test_short_or_cap_limited_pages_cannot_publish_exact_coverage(self):
        self.pages={1:([row(n) for n in range(1,101)],{'Link':f'<{page_url(2)}>; rel="next"'})}
        self.step(max_pages=1);self.assert_no_watermark();self.assertEqual(self.decode()[0],[])

    def test_repeated_native_ids_or_page_jump_fail_closed(self):
        self.pages={1:([row(n) for n in range(1,101)],{'Link':f'<{page_url(2)}>; rel="next"'}),
                    2:([row(1)],{})}
        self.step();self.step();self.assert_no_watermark()

    def test_malformed_nonempty_link_is_not_a_terminal_proof(self):
        self.pages={1:([row(1)],{'Link':'not a valid pagination Link'})}
        self.step();self.assert_no_watermark()

    def test_foreign_link_or_skipped_offset_never_advance(self):
        self.pages={1:([row(n) for n in range(1,101)],{'Link':f'<{page_url(3)}>; rel="next"'})}
        self.step();self.assert_no_watermark()

    def test_empty_native_result_requires_same_cycle_identity_proof(self):
        self.pages={1:([],{})};self.step();self.assert_no_watermark();self.step()
        original=self.state();self.assertEqual(len(original['completed_cycles']),1)
        self.assertIn(('tool','pr_creation'),self.decode(original)[1])
        identity=original['requests'][-1];sidecar=self.output/(identity['raw_path']+'.manifest.json')
        changes={'cycle_id':'other-cycle','tool_id':'other-tool','github_repo_id':99,
            'window_since':'2023-01-01T00:00:00Z','window_end_exclusive':'2023-01-22T00:00:00Z',
            'request_url':'https://example.org/repos/org/repo','observed_at':'2023-01-22T12:00:00Z'}
        for field,value in changes.items():
            state=copy.deepcopy(original);state['requests'][-1][field]=value
            sidecar.write_text(json.dumps(state['requests'][-1]))
            with self.subTest(field=field),self.assertRaises(ValueError):self.decode(state)
        sidecar.write_text(json.dumps(identity))

    def test_altered_raw_or_terminal_count_is_rejected_offline(self):
        self.step();state=self.state();state['completed_cycles'][0]['pages']+=1
        with self.assertRaises(ValueError):self.decode(state)
        state=self.state();path=self.output/state['requests'][0]['raw_path'];path.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'hash differs'):self.decode()


class SharedDispatchAdversarial(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.calls=[]
    def worker(self,*args):
        cap=args[3];self.calls.extend([1]*cap);return {'requests':cap,'status':'fixture_success'}
    def dispatch(self,now=NOW):return dispatcher.run(self.root/'registry',self.root,self.root/'baseline',4,0,lambda url:None,now)

    def test_total_cap_is_shared_between_streams_and_repeated_manual_dispatches(self):
        with mock.patch.object(dispatcher.native,'run',side_effect=self.worker),mock.patch.object(dispatcher.issues,'run',side_effect=self.worker):
            self.assertEqual(self.dispatch()['requests'],4);self.assertEqual(self.dispatch(NOW+timedelta(minutes=1))['requests'],0)
            self.assertEqual(len(self.calls),4);self.assertEqual(self.dispatch(NOW+timedelta(minutes=15))['requests'],4)

    def test_first_stream_cooldown_stops_the_second_stream(self):
        def paused(*args):self.calls.append(1);return {'requests':1,'not_before':r.iso(NOW+timedelta(minutes=2))}
        with mock.patch.object(dispatcher.native,'run',side_effect=paused),mock.patch.object(dispatcher.issues,'run') as issues:
            self.assertEqual(self.dispatch()['requests'],1);issues.assert_not_called()
            self.assertEqual(self.dispatch(NOW+timedelta(minutes=1))['requests'],0)

    def test_archived_calls_reconstruct_missing_dispatch_counter(self):
        (self.root/native.STATE).write_text(json.dumps({'requests':[{'observed_at':r.iso(NOW)}]*3}))
        with mock.patch.object(dispatcher.native,'run',side_effect=self.worker),mock.patch.object(dispatcher.issues,'run',side_effect=self.worker):
            self.assertEqual(self.dispatch()['requests'],1);self.assertEqual(len(self.calls),1)

    def test_manual_and_cron_entrypoints_cannot_spend_the_same_window_concurrently(self):
        entered=threading.Event();release=threading.Event()
        def slow(*args):
            entered.set();release.wait(3);return self.worker(*args)
        with mock.patch.object(dispatcher.native,'run',side_effect=slow),mock.patch.object(dispatcher.issues,'run',side_effect=self.worker):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(self.dispatch);self.assertTrue(entered.wait(2));second=pool.submit(self.dispatch)
                try:result=second.result(timeout=1);self.assertEqual(result['requests'],0)
                finally:release.set()
                first.result(timeout=3)
        self.assertLessEqual(len(self.calls),4)

    def test_unknown_worker_attempt_is_not_refunded_on_same_window_retry(self):
        def failed(*args):self.calls.append(1);raise RuntimeError('unknown result after a provider attempt')
        with mock.patch.object(dispatcher.native,'run',side_effect=failed),mock.patch.object(dispatcher.issues,'run',side_effect=self.worker):
            with self.assertRaises(RuntimeError):self.dispatch()
        path=self.root/'activity_dispatch_state.json'
        self.assertTrue(path.is_file(),'Unknown request allotment must survive worker failure')
        with mock.patch.object(dispatcher.native,'run',side_effect=self.worker),mock.patch.object(dispatcher.issues,'run',side_effect=self.worker):self.dispatch()
        self.assertLessEqual(len(self.calls),4)


if __name__=='__main__':unittest.main()
