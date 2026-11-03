"""Offline backfill checkpoint, idempotence, split, and budget checks."""

from datetime import date
import json
from pathlib import Path
import tempfile

from backfill import SCHEMA, add_job, connect, plan, reserve, save, split


def load_job(root,identifier):
    with connect(read_only=False,data_dir=root) as con:
        result=con.execute("SELECT * FROM backfill.jobs WHERE job_id=?",[identifier])
        return dict(zip([c[0] for c in result.description],result.fetchone()))


def main():
    with tempfile.TemporaryDirectory(prefix="6893-backfill-check-") as folder:
        root=Path(folder)
        with connect(read_only=False,data_dir=root) as con:
            con.execute(SCHEMA)
            add_job(con,"testplan","arxiv","tool",date(2023,1,1),date(2023,2,1),"history",1)
            identifier=con.execute("SELECT job_id FROM backfill.jobs").fetchone()[0]
        job=load_job(root,identifier)
        assert reserve(root,job,100,200)==100
        assert reserve(root,job,80,200)==100
        assert reserve(root,{"job_id":"other"},150,200) is None
        assert reserve(root,job,120,200) is None
        result={"raw_bytes":b"fixture-page-one","papers":[{"id":"fixture:1","requires_manual_review":True}],"pagination":{"complete":False,"next_cursor":100}}
        assert save(root,job,result)==1
        assert load_job(root,identifier)["cursor"]==100
        assert load_job(root,identifier)["state"]=="pending"
        save(root,job,result)
        with connect(data_dir=root) as con:
            assert con.execute("SELECT count(*) FROM backfill.records").fetchone()[0]==1
        second=load_job(root,identifier)
        save(root,second,{"raw_bytes":b"fixture-page-two","papers":[{"id":"fixture:2"}],"pagination":{"complete":True,"next_cursor":None}})
        assert load_job(root,identifier)["state"]=="done"
        with connect(data_dir=root) as con:
            assert con.execute("SELECT count(*) FROM backfill.records").fetchone()[0]==2
        with connect(read_only=False,data_dir=root) as con:
            add_job(con,"testplan","arxiv","splittool",date(2023,1,1),date(2023,2,1),"history",1)
            split_id=con.execute("SELECT job_id FROM backfill.jobs WHERE tool_id='splittool'").fetchone()[0]
        split(root,load_job(root,split_id))
        with connect(data_dir=root) as con:
            ranges=con.execute("SELECT start_date,end_date FROM backfill.jobs WHERE tool_id='splittool' AND state='pending' ORDER BY start_date").fetchall()
            assert ranges[0][0]==date(2023,1,1) and ranges[-1][1]==date(2023,2,1)
            assert ranges[0][1]==ranges[1][0]
    print(json.dumps({"status":"passed","isolated_fixtures_removed":True,"checkpoint_pagination":True,"idempotent_page_storage":True,"split_without_gaps":True,"budget_reservations_bounded":True,"frozen_execution_cap":True}))


if __name__=="__main__":
    main()
