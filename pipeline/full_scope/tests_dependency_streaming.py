import argparse
from collections import defaultdict
import copy
import contextlib
import gzip
import importlib.util
import json
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import build_dependency_features as reference
import build_dependency_features_streaming as streaming
from dependency_sources import canonical
from tests_dependencies import artifact


REGISTRY={'tools':[{'tool_id':'pytorch','pypi_package':'torch'},{'tool_id':'mlflow','pypi_package':'mlflow'}]}
OBSERVED='2023-03-01T00:00:00Z'


def rows():
    output=[artifact('torch','0','2022-01-01T00:00:00Z',['numpy']),
        artifact('mlflow','0','2022-01-01T00:00:00Z',["pandas; extra == 'train'"]),
        artifact('consumerA','0','2022-12-01T00:00:00Z',['torch>=1','mlflow']),
        artifact('consumerA','1','2023-01-04T00:00:00Z',[]),
        artifact('consumerA','2','2023-01-11T00:00:00Z',["torch; extra == 'train'"]),
        artifact('consumerB','1','2023-01-02T00:00:00Z',[],'first.whl'),
        artifact('consumerB','1','2023-01-10T00:00:00Z',['torch'],'second.whl'),
        artifact('consumerB','0.5','2023-01-16T00:00:00Z',['mlflow']),
        artifact('consumerC','1','2023-01-10T00:00:00Z',['torch >>> nope']),
        artifact('consumerD','1',None,['mlflow']),
        artifact('not a package','1','2023-01-10T00:00:00Z',['torch']),
        artifact('consumerE','1','2023-01-20T00:00:00Z',[';'])]
    output.append(copy.deepcopy(output[4]))
    return output


class Streaming(unittest.TestCase):
    def test_per_package_sweep_matches_whole_history_reference(self):
        raw=rows();artifacts,edges,audit=reference.normalize_artifacts(raw,OBSERVED,'2023-03-01')
        expected=reference.weekly_features(artifacts,REGISTRY,'2023-01-02','2023-02-27',True)
        groups=defaultdict(list)
        for row in raw:groups[row['name'].lower()].append(row)
        found_artifacts=[];found_edges=[];found_errors=[]
        actual,found=streaming.grouped_features(sorted(groups.items()),REGISTRY,OBSERVED,'2023-01-02','2023-02-27',True,
            found_artifacts.append,found_edges.append,found_errors.append)
        self.assertEqual(actual,expected)
        self.assertEqual(sorted(reference.encode(a) for a in found_artifacts),sorted(reference.encode({k:v for k,v in a.items() if k!='parsed_requirements'}) for a in artifacts))
        originals={a['artifact_id']:a for a in artifacts}
        for item in found_artifacts:
            self.assertEqual(streaming.reconstruct_requirements(item,found_edges,found_errors),originals[item['artifact_id']]['parsed_requirements'])
        self.assertEqual(sorted(reference.encode(a) for a in found_edges),sorted(reference.encode(a) for a in edges))
        self.assertEqual(found['error_counts'],audit['error_counts'])
        self.assertEqual(found['input_rows'],len(raw));self.assertEqual(found['exact_duplicate_rows_removed'],1)

    def test_outbound_only_mode_never_claims_complete_inbound_history(self):
        raw=rows()[:2];groups=[(row['name'],[row]) for row in raw]
        actual,_=streaming.grouped_features(groups,REGISTRY,OBSERVED,'2023-01-02','2023-02-27',False,
            lambda x:None,lambda x:None,lambda x:None)
        self.assertTrue(all(row['dep_ever_declaring_packages'] is None for row in actual))
        self.assertFalse(any(row['dep_inbound_event_time_eligible'] for row in actual))

    def test_many_foreign_requirements_stream_all_edges_without_changing_cohort_features(self):
        foreign=['foreign%d>=1'%i for i in range(1000)]
        raw=[artifact('torch','0','2022-01-01T00:00:00Z',['numpy']),
             artifact('mlflow','0','2022-01-01T00:00:00Z',['pandas']),
             artifact('consumer','0','2022-12-01T00:00:00Z',['torch',*foreign]),
             artifact('consumer','1','2023-01-11T00:00:00Z',foreign)]
        artifacts,edges,_=reference.normalize_artifacts(raw,OBSERVED,'2023-03-01')
        expected=reference.weekly_features(artifacts,REGISTRY,'2023-01-02','2023-02-27',True)
        source_groups=defaultdict(list)
        for row in raw:source_groups[row['name']].append(row)
        emitted=[];exported=[]
        actual,audit=streaming.grouped_features(sorted(source_groups.items()),REGISTRY,OBSERVED,'2023-01-02','2023-02-27',True,
            exported.append,emitted.append,lambda x:None)
        self.assertEqual(actual,expected);self.assertEqual(len(emitted),len(edges))
        self.assertEqual(audit['parsed_edges'],len(edges))
        self.assertEqual(sorted(reference.encode(r) for r in exported),sorted(reference.encode({k:v for k,v in r.items() if k!='parsed_requirements'}) for r in artifacts))
        originals={a['artifact_id']:a for a in artifacts}
        for item in exported:self.assertEqual(streaming.reconstruct_requirements(item,emitted,[]),originals[item['artifact_id']]['parsed_requirements'])
        compact=streaming.compact_for_features(artifacts,REGISTRY)
        self.assertEqual(sum(len(a['parsed_requirements']) for a in compact if a['package']=='consumer'),1)
        streamed=[];_,retained,audit=reference.normalize_artifacts(raw,OBSERVED,'2023-03-01',edge_sink=streamed.append)
        self.assertEqual(retained,[]);self.assertEqual(len(streamed),len(edges));self.assertEqual(audit['parsed_edges'],len(edges))

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB unavailable in this test environment')
    def test_external_sort_preserves_complete_source_rows_and_bounds_groups(self):
        import duckdb,gzip
        from dependency_sources import FIELDS
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'source.jsonl.gz';raw=[]
            for item in reversed(rows()):
                row={name:item.get(name) for name in FIELDS}
                for field in ['requires_dist','classifiers','project_urls']:
                    if row[field] is None and field!='requires_dist':row[field]=[]
                raw.append(row)
            path.write_bytes(gzip.compress(''.join(json.dumps(row)+'\n' for row in raw).encode()))
            connection=duckdb.connect()
            groups=list(streaming.sorted_groups(connection,path,100))
            flattened=[row for key,group in groups for row in group]
            self.assertEqual(sorted(reference.encode(a) for a in raw),sorted(reference.encode(a) for a in flattened))
            self.assertEqual([key for key,_ in groups],sorted(key for key,_ in groups))
            with self.assertRaisesRegex(ValueError,'memory bound'):
                list(streaming.sorted_groups(connection,path,1))
            connection.close()

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB unavailable in this test environment')
    def test_streamed_publication_binds_receipt_artifacts_and_quarantine(self):
        from dependency_sources import FIELDS,build_metadata_query
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);raw=[]
            for item in rows():
                row={name:item.get(name) for name in FIELDS}
                for field in ['classifiers','project_urls']:row[field]=[]
                raw.append(row)
            source=root/'source.jsonl.gz';source.write_bytes(gzip.compress(''.join(json.dumps(r)+'\n' for r in raw).encode()))
            registry=root/'registry.json';registry.write_text(json.dumps(REGISTRY))
            plan=build_metadata_query(REGISTRY,'2023-03-01');plan_path=root/'query_plan.json';plan_path.write_text(json.dumps(plan))
            receipt=root/'receipt.json';receipt.write_text(json.dumps({'query_sha256':plan['query_sha256'],
                'job_complete':True,'all_pages_fetched':True,'total_rows':len(raw),'observed_at':OBSERVED,
                'output_sha256':reference.sha(source)}))
            args=argparse.Namespace(input=source,registry=registry,query_plan=plan_path,query_receipt=receipt,
                output=root/'published',start='2023-01-02',end_exclusive='2023-03-01',observed_at=None,
                memory_limit='512MB',max_temp_gib=1,minimum_free_gib=15,max_package_rows=100)
            with mock.patch.object(streaming.shutil,'disk_usage',return_value=argparse.Namespace(free=100*1024**3)),contextlib.redirect_stdout(io.StringIO()):
                streaming.build(args)
            manifest=json.loads((args.output/'manifest.json').read_text())
            quality=json.loads((args.output/'dependency_quality.json').read_text())
            self.assertEqual(manifest['status'],'needs_review')
            self.assertEqual(manifest['format_version'],2);self.assertEqual(quality['format_version'],2)
            self.assertEqual(quality['audit']['input_rows'],len(raw));self.assertEqual(quality['audit']['unique_artifacts'],len(raw)-1)
            for name,digest in manifest['files'].items():self.assertEqual(reference.sha(args.output/name),digest)
            with gzip.open(args.output/'parse_errors.jsonl.gz','rt') as stream:errors=[json.loads(line) for line in stream]
            self.assertEqual(len(errors),quality['audit']['error_count'])
            self.assertGreater(len(errors),0)
            with gzip.open(args.output/'artifacts.jsonl.gz','rt') as stream:exported=[json.loads(line) for line in stream]
            with gzip.open(args.output/'declared_dependency_edges.jsonl.gz','rt') as stream:edges=[json.loads(line) for line in stream]
            original,_,_=reference.normalize_artifacts(raw,OBSERVED,'2023-03-01');lookup={a['artifact_id']:a for a in original}
            for item in exported:
                self.assertNotIn('parsed_requirements',item)
                self.assertEqual(streaming.reconstruct_requirements(item,edges,errors),lookup[item['artifact_id']]['parsed_requirements'])

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB unavailable in this test environment')
    def test_large_payload_sort_preserves_groups_under_production_memory_cap(self):
        import duckdb
        from dependency_sources import FIELDS
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);source=root/'large.jsonl.gz';spill=root/'spill';spill.mkdir()
            with gzip.open(source,'wt') as stream:
                for index in range(100000):
                    item={name:None for name in FIELDS}
                    item.update(name='package-%04d'%(499-index%500),version=str(index),requires_dist=['torch>=1'],
                        classifiers=[],project_urls=[],summary='payload-'+str(index)+'x'*2048,
                        filename=str(index)+'.whl',path='/files/'+str(index),upload_time='2023-01-01T00:00:00Z')
                    stream.write(json.dumps(item)+'\n')
            connection=duckdb.connect();connection.execute("SET memory_limit='3GB'")
            connection.execute('SET threads=1');connection.execute('SET preserve_insertion_order=false')
            connection.execute('SET temp_directory='+streaming.sql_string(spill))
            connection.execute("SET max_temp_directory_size='1GiB'")
            total=0;previous=None;peak_spill=0
            for package,items in streaming.sorted_groups(connection,source,1000):
                self.assertTrue(previous is None or package>=previous);previous=package
                total+=len(items);self.assertEqual(len(items),200)
                peak_spill=max(peak_spill,sum(p.stat().st_size for p in spill.iterdir() if p.is_file()))
            self.assertEqual(total,100000);self.assertLessEqual(peak_spill,1024**3)
            connection.close()


if __name__=='__main__':unittest.main(verbosity=2)
