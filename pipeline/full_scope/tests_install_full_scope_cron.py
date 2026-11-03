import unittest
from install_full_scope_cron import replace_block,BEGIN,END,SCHEDULES


class CronBlock(unittest.TestCase):
    def test_six_unique_workers_include_bounded_release_dispatch(self):
        self.assertEqual(len(SCHEDULES),6)
        self.assertEqual(len({row[1] for row in SCHEDULES}),6)
        self.assertIn(('13,28,43,58 * * * *','run-github-releases.sh','releases'),SCHEDULES)
    def test_unrelated_jobs_preserved_and_reinstall_idempotent(self):
        old='MAILTO=""\n*/15 * * * * /existing-job\n'
        once=replace_block(old,['new-entry'])
        self.assertEqual(once,replace_block(once,['new-entry']))
        self.assertIn('/existing-job',once);self.assertEqual(once.count('new-entry'),1)

    def test_only_owned_block_replaced(self):
        result=replace_block('first\n'+BEGIN+'\nold\n'+END+'\nlast\n',['new'])
        self.assertEqual(result,'first\nlast\n'+BEGIN+'\nnew\n'+END+'\n')

    def test_invalid_block_refuses_mutation(self):
        for text in [BEGIN+'\nold',END+'\n',BEGIN+'\n'+BEGIN+'\n'+END]:
            with self.subTest(text=text),self.assertRaises(ValueError):replace_block(text,['new'])


if __name__=='__main__':unittest.main()
