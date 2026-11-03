import unittest
from github_issues_cursor import cursor,validate_page,check_context
class CursorTests(unittest.TestCase):
 def test_exact_real_provider_tokens(self):
  self.assertEqual(cursor('2023-01-02T19:22:03Z',945900344),'Y3Vyc29yOnYyOpLPAAABhXPvB3jOOGFLOA==')
  self.assertEqual(cursor('2023-01-13T10:23:42Z',1523201620),'Y3Vyc29yOnYyOpLPAAABhaqoG7DOWso2VA==')
 def test_provider_tie_order_can_decrease_id(self):
  anchor={'id':1523201620,'updated_at':'2023-01-13T10:23:42Z'}
  row={'id':1523196084,'updated_at':'2023-01-13T10:23:42Z'}
  result=validate_page([row],anchor)
  self.assertEqual(result['last_anchor'],row);self.assertNotEqual(result['cursor_out'],cursor(anchor['updated_at'],anchor['id']))
 def test_timestamp_regression_and_repeated_boundary_fail(self):
  anchor={'id':10,'updated_at':'2023-02-01T00:00:00Z'}
  with self.assertRaisesRegex(ValueError,'decreased'):validate_page([{'id':11,'updated_at':'2023-01-31T23:59:59Z'}],anchor)
  with self.assertRaisesRegex(ValueError,'repeated'):validate_page([dict(anchor)],anchor)
 def test_duplicate_id_and_true_empty_terminal(self):
  row={'id':1,'updated_at':'2023-01-01T00:00:00Z'}
  with self.assertRaisesRegex(ValueError,'Duplicate'):validate_page([row,row])
  self.assertFalse(validate_page([row])['terminal_page']);self.assertTrue(validate_page([],row)['terminal_page'])
 def test_frozen_all_state_filter_preserved(self):
  base='https://api.github.com/repos/a/b/issues?state=all&sort=updated&direction=asc&since=2022-12-31T23%3A59%3A59Z&per_page=100&after=publiccursor'
  self.assertTrue(check_context(base,'a/b','2022-12-31T23:59:59Z'))
  for bad in (base.replace('state=all','state=open'),base.replace('sort=updated','sort=created'),base.replace('per_page=100','per_page=28'),base.replace('direction=asc','direction=desc')):
   with self.assertRaisesRegex(ValueError,'filters'):check_context(bad,'a/b','2022-12-31T23:59:59Z')
 def test_boundary_overlap_keeps_exact_jan1(self):
  self.assertFalse(validate_page([{'id':1,'updated_at':'2023-01-01T00:00:00Z'}])['terminal_page'])
if __name__=='__main__':unittest.main()
