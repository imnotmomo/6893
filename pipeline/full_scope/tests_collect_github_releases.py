from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from archive_github_releases import Archive
import collect_github_releases as module

UTC=timezone.utc


class PublicReleaseCollector(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.project=Path(self.temp.name);(self.project/'config').mkdir()
        self.registry=self.project/'config/tools.study.json'
        self.tools=[{'tool_id':'t'+str(i),'github_repo':'org/t'+str(i),'github_repo_id':100+i,'pypi_package':'t'+str(i)} for i in range(30)]
        self.registry.write_text(json.dumps({'tools':self.tools}))
        self.root=self.project/'data/raw/github_releases';self.root.mkdir(parents=True)
        self.now=datetime(2026,11,3,6,tzinfo=UTC);self.calls=[]

    def fetch(self,url):
        self.calls.append(url)
        if '/releases?' in url:body=[]
        else:
            name=url.rsplit('/',1)[-1];body={'id':100+int(name[1:]),'full_name':'org/'+name}
        return {'status':200,'body':json.dumps(body).encode(),'headers':{},'url':url}

    def test_request_cap_preserves_cursor_and_saved_raw(self):
        first=module.run(self.project,1,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(first['requests'],1);self.assertEqual(len(self.calls),1)
        body=module.read(Path(first['active_root'])/'collection_manifest.json')
        self.assertEqual(body['requests'][0]['phase'],'identity')
        with patch.object(module.time,'sleep'):
            second=module.run(self.project,3,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(second['requests'],3);self.assertEqual(second['daily_used'],4)
        body=module.read(Path(first['active_root'])/'collection_manifest.json')
        self.assertEqual([r['phase'] for r in body['requests'][:3]],['identity','list','anchor'])

    def test_daily_quota_is_shared_across_invocations_and_reset_by_utc_day(self):
        state={'source':module.SOURCE,'registry_sha256':module.sha(self.registry),
               'quota':{'utc_date':self.now.date().isoformat(),'used':39}}
        module.atomic(self.root/'refresh_state.json',state)
        result=module.run(self.project,4,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(result['requests'],1);self.assertEqual(result['daily_used'],40)
        result=module.run(self.project,4,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(result['status'],'daily_quota_wait');self.assertEqual(len(self.calls),1)
        with patch.object(module.time,'sleep'):
            result=module.run(self.project,1,fetch=self.fetch,now=self.now+timedelta(days=1),execute=True)
        self.assertEqual(result['daily_used'],1)

    def test_rate_limit_headers_stop_before_additional_requests_or_cursor_advance(self):
        def limited(url):
            self.calls.append(url)
            return {'status':429,'body':b'{"message":"limited"}','url':url,
                    'headers':{'X-RateLimit-Remaining':'0','X-RateLimit-Reset':str(int((self.now+timedelta(hours=1)).timestamp()))}}
        result=module.run(self.project,4,fetch=limited,now=self.now,execute=True)
        self.assertEqual(result['requests'],1)
        body=module.read(Path(result['active_root'])/'collection_manifest.json')
        self.assertEqual(body['requests'][0]['phase'],'error')
        result=module.run(self.project,4,fetch=limited,now=self.now+timedelta(minutes=1),execute=True)
        self.assertEqual(result['status'],'rate_wait');self.assertEqual(len(self.calls),1)

    def test_transport_failure_consumes_one_request_and_defers(self):
        def failed(url):self.calls.append(url);raise OSError('fixture failure')
        result=module.run(self.project,4,fetch=failed,now=self.now,execute=True)
        self.assertEqual(result['requests'],1);self.assertEqual(len(self.calls),1)
        self.assertEqual(module.read(Path(result['active_root'])/'collection_manifest.json')['requests'][0]['http_status'],0)

    def test_changed_registry_fails_before_network(self):
        module.atomic(self.root/'refresh_state.json',{'source':module.SOURCE,'registry_sha256':'different'})
        with self.assertRaisesRegex(ValueError,'cohort changed'):
            module.run(self.project,1,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(self.calls,[])

    def test_oversized_request_configuration_is_rejected(self):
        for requests,daily in [(5,40),(1,41),(0,40)]:
            with self.assertRaises(ValueError):module.run(self.project,requests,daily,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(self.calls,[])

    def test_plan_never_fetches_and_free_space_guard_precedes_http(self):
        result=module.run(self.project,1,fetch=self.fetch,now=self.now)
        self.assertEqual(result['status'],'plan');self.assertEqual(self.calls,[])
        with patch.object(module.shutil,'disk_usage',return_value=type('Disk',(),{'free':0})()),self.assertRaisesRegex(ValueError,'free guard'):
            module.run(self.project,1,fetch=self.fetch,now=self.now,execute=True)
        self.assertEqual(self.calls,[])

    def test_instability_starts_a_new_preserved_attempt_only_after_cooldown(self):
        archive=Archive(self.root/'fixture',self.registry,'2026-11-03')
        tool=self.tools[0]
        for phase,body in [('identity',{'id':100,'full_name':'org/t0'}),('list',[]),
                           ('anchor',[{'id':1,'url':'https://api.github.com/repos/org/t0/releases/1','draft':False,'prerelease':False,'published_at':'2023-01-04T00:00:00Z'}])]:
            archive.save({'tool_id':'t0','kind':'repository' if phase=='identity' else 'releases','phase':phase,
                'page':1,'per_page':100,'url':'https://api.github.com/repos/org/t0'+('' if phase=='identity' else '/releases?per_page=100&page=1'),
                'observed_at':self.now.isoformat(),'body':json.dumps(body)})
        archive.checkpoint()
        self.assertIsNone(module.next_request(archive,tool,self.now))
        self.assertEqual(len(archive.state['requests']),3)
        self.assertIsNone(module.next_request(archive,tool,self.now+timedelta(hours=1)))
        request=module.next_request(archive,tool,self.now+timedelta(days=1))
        self.assertEqual(request['phase'],'identity');self.assertEqual(request['attempt'],2)

    def test_identical_bodies_keep_distinct_observation_provenance_after_interrupted_checkpoint(self):
        archive=Archive(self.root/'fixture',self.registry,'2026-11-03')
        item={'tool_id':'t0','kind':'repository','phase':'identity','page':1,'per_page':100,
              'url':'https://api.github.com/repos/org/t0','observed_at':self.now.isoformat(),
              'body':json.dumps({'id':100,'full_name':'org/t0'})}
        archive.save(item);first=archive.state['requests'][0]['raw_path']
        archive.state['requests']=[]
        archive.save({**item,'observed_at':(self.now+timedelta(minutes=1)).isoformat()})
        second=archive.state['requests'][0]['raw_path']
        self.assertNotEqual(first,second);self.assertTrue((archive.root/first).is_file());self.assertTrue((archive.root/second).is_file())


if __name__=='__main__':unittest.main(verbosity=2)
