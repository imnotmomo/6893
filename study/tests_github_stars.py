import gzip
import json
from datetime import timedelta
from pathlib import Path
import tempfile
import unittest

from collect_github_stars import (Collector, WEEK, iso, now, parse_gh_response,
                                  retry_at, stamp, validate_history, validate_rows)

def entry(week, total=1):
    return {'week': week, 'total': total, 'days': [total, 0, 0, 0, 0, 0, 0]}

class HistoryValidation(unittest.TestCase):
    def test_valid_zero_weeks_survive_but_current_partial_is_excluded(self):
        first=int(stamp('2023-01-01').timestamp())
        summary, rows=validate_history([entry(first+2*WEEK,2),entry(first+WEEK,0),entry(first)],
            stamp('2022-12-31'),stamp('2023-01-01'),stamp('2023-01-17'),3)
        self.assertEqual(summary['excluded_current_partial_weeks'],1)
        self.assertEqual([r['stars'] for r in rows],[0,1])
        self.assertFalse(summary['historical_vintage'])
        self.assertFalse(summary['primary_eligible'])
        self.assertFalse(summary['boundary_alignment_certified_utc'])
        self.assertFalse(rows[0]['alignment_eligible_as_of'])
    def test_bad_total_negative_noninteger_and_duplicate_anchors_rejected(self):
        epoch=int(stamp('2023-01-01').timestamp())
        malformed=[{'week':epoch,'total':2,'days':[1,0,0,0,0,0,0]},
                   {'week':epoch,'total':-1,'days':[-1,0,0,0,0,0,0]},
                   {'week':epoch,'total':1,'days':[True,0,0,0,0,0,0]}]
        for bad in malformed:
            with self.assertRaises(ValueError):validate_rows([bad])
        with self.assertRaisesRegex(ValueError,'Duplicate'):validate_rows([entry(epoch),entry(epoch)])
        with self.assertRaisesRegex(ValueError,'non-604800'):validate_rows([entry(epoch+WEEK+3600),entry(epoch)])
    def test_creation_bounds_and_required_start_rejected(self):
        epoch=int(stamp('2023-01-01').timestamp())
        with self.assertRaisesRegex(ValueError,'before repository creation'):
            validate_history([entry(epoch+WEEK),entry(epoch)],stamp('2023-01-09'),stamp('2023-01-01'),stamp('2023-01-16'),3)
        with self.assertRaisesRegex(ValueError,'start/creation bucket'):
            validate_history([entry(epoch+WEEK)],stamp('2022-01-01'),stamp('2023-01-01'),stamp('2023-01-16'),3)
    def test_parse_headers_does_not_retain_cookies_or_auth(self):
        status,headers,body=parse_gh_response(b'HTTP/2.0 200 OK\r\nETag: public-hash\r\nSet-Cookie: private\r\nAuthorization: secret\r\n\r\n[]')
        self.assertEqual(status,200);self.assertEqual(body,b'[]')
        self.assertEqual(headers,{'etag':'public-hash'})
    def test_retry_after_defers_without_sleep(self):
        after=retry_at(429,{'retry-after':'3600'},'2023-01-01T00:00:00Z')
        self.assertEqual(after,'2023-01-01T01:00:00Z')

class CollectorFixtures(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.registry=self.root/'registry.json'
        self.registry.write_text(json.dumps({'tools':[{'tool_id':'tool','github_repo':'org/tool','github_repo_id':7}]}))
        self.epoch=int(stamp('2023-01-01').timestamp())
        self.calls=[]
    def tearDown(self):self.temp.cleanup()
    def response(self,body,status=200,headers=None):
        return {'status':status,'headers':headers or {},'body':json.dumps(body).encode(),
                'requested_at':iso(now()),'observed_at':iso(now())}
    def fetch(self,path,transport):
        self.calls.append(path)
        if '/history?' not in path:return self.response({'id':7,'full_name':'org/renamed','created_at':'2022-01-01T00:00:00Z'})
        return self.response([entry(self.epoch+WEEK,0),entry(self.epoch,2)])
    def collector(self,**kw):
        return Collector(self.root/'output',self.registry,'2023-01-01','2023-01-16',fetch=self.fetch,**kw)
    def test_resume_uses_verified_immutable_raw_and_numeric_identity(self):
        result=self.collector().run()
        self.assertEqual(result['status'],'passed',result)
        self.assertEqual(result['tools_passed'],1)
        self.assertIn('/repos/org/renamed/stargazers/history?per_page=30&page=1',self.calls)
        paths=list((self.root/'output/raw/tool').glob('*.gz'))
        original={p.name:p.read_bytes() for p in paths}
        self.assertEqual(len(paths),2)
        self.calls.clear();resumed=self.collector().run()
        self.assertEqual(resumed['status'],'passed');self.assertEqual(self.calls,[])
        self.assertEqual(original,{p.name:p.read_bytes() for p in paths})
        raw=paths[0];raw.write_bytes(gzip.compress(b'{}'))
        failed=self.collector().run()
        self.assertEqual(failed['status'],'partial')
        self.assertIn('checksum mismatch',failed['tools']['tool']['reason'])
    def test_wrong_repository_id_fails_before_history_collection(self):
        def wrong(path,transport):return self.response({'id':999,'full_name':'org/tool','created_at':'2022-01-01'})
        collector=self.collector();collector.fetch=wrong
        result=collector.run();self.assertEqual(result['status'],'partial')
        self.assertIn('numeric identity',result['tools']['tool']['reason'])
        self.assertEqual(len(result['requests']),1)
    def test_rate_limit_has_no_busy_retry_and_preserves_error_response(self):
        def limited(path,transport):
            self.calls.append(path)
            return self.response({'message':'rate limited'},429,{'retry-after':'3600'})
        collector=self.collector();collector.fetch=limited
        result=collector.run();self.assertEqual(result['status'],'partial');self.assertEqual(len(self.calls),1)
        collector=self.collector();collector.fetch=limited
        result=collector.run();self.assertEqual(len(self.calls),1)
        self.assertEqual(result['tools']['tool']['status'],'deferred')
        self.assertEqual(len(list((self.root/'output/raw/tool').glob('*.gz'))),1)
    def test_request_limit_resumes_at_unfetched_page(self):
        result=self.collector(max_requests=1).run()
        self.assertEqual(result['tools']['tool']['status'],'deferred')
        self.assertEqual(len(self.calls),1)
        result=self.collector(max_requests=1).run()
        self.assertEqual(result['status'],'passed',result)
        self.assertEqual(len(self.calls),2)
    def test_404_is_retained_without_repeated_requests(self):
        def absent(path,transport):self.calls.append(path);return self.response({'message':'Not Found'},404)
        collector=self.collector();collector.fetch=absent
        self.assertEqual(collector.run()['status'],'partial')
        collector=self.collector();collector.fetch=absent
        self.assertEqual(collector.run()['status'],'partial');self.assertEqual(len(self.calls),1)
    def test_pagination_crosses_pages_continuously_and_rejects_overlap(self):
        records=[entry(self.epoch+n*WEEK,0 if n==20 else 1) for n in range(31,-1,-1)]
        def paged(path,transport):
            if '/history?' not in path:return self.response({'id':7,'full_name':'org/tool','created_at':'2022-01-01'})
            return self.response(records[:30] if path.endswith('page=1') else records[30:])
        args={'registry':self.registry,'start':'2023-01-01',
              'as_of':iso(stamp('2023-01-01')+timedelta(weeks=31,days=2)), 'fetch':paged}
        collector=Collector(self.root/'pages',**args)
        result=collector.run()
        self.assertEqual(result['status'],'passed',result)
        self.assertEqual(result['tools']['tool']['pages'],2)
        self.assertEqual(result['weekly_rows'],31)
        self.assertEqual(result['tools']['tool']['duplicate_weeks'],0)
        def overlap(path,transport):
            if '/history?' not in path:return paged(path,transport)
            return self.response(records[:30] if path.endswith('page=1') else records[29:])
        args['fetch']=overlap
        result=Collector(self.root/'overlap',**args).run()
        self.assertEqual(result['status'],'partial')
        self.assertIn('Duplicate',result['tools']['tool']['reason'])

if __name__=='__main__':unittest.main()
