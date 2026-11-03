"""Offline audit regressions for BigQuery's typed-parameter serialization."""
import copy
import unittest

from audit_collection import verify_returned_query


class ReturnedQueryParameters(unittest.TestCase):
    def setUp(self):
        self.cap=10*1024**2
        self.expected=[
            {'name':'start_date','parameterType':{'type':'DATE'},'parameterValue':{'value':'2023-01-02'}},
            {'name':'end_date','parameterType':{'type':'DATE'},'parameterValue':{'value':'2023-01-09'}},
            {'name':'repository_ids','parameterType':{'type':'ARRAY','arrayType':{'type':'INT64'}},'parameterValue':{'arrayValues':[{'value':'65600975'}]}},
            {'name':'repository_names','parameterType':{'type':'ARRAY','arrayType':{'type':'STRING'}},'parameterValue':{'arrayValues':[]}},
            {'name':'tool_ids','parameterType':{'type':'ARRAY','arrayType':{'type':'STRING'}},'parameterValue':{'arrayValues':[{'value':'pytorch'},{'value':'tensorflow'}]}},
        ]

    def query(self):
        return {'maximumBytesBilled':str(self.cap),'queryParameters':copy.deepcopy(self.expected)}

    def test_google_omitted_empty_array_values_or_parameter_value_are_equivalent(self):
        for replacement in [{},None]:
            with self.subTest(replacement=replacement):
                query=self.query();parameter=query['queryParameters'][3]
                if replacement is None:parameter.pop('parameterValue')
                else:parameter['parameterValue']=replacement
                query['queryParameters'].reverse()
                verify_returned_query(query,self.cap,self.expected)

    def test_changed_nonempty_tool_ids_are_rejected(self):
        query=self.query();query['queryParameters'][4]['parameterValue']['arrayValues'][0]['value']='unapproved-tool'
        with self.assertRaises(ValueError):verify_returned_query(query,self.cap,self.expected)

    def test_changed_nonempty_repository_id_is_rejected(self):
        query=self.query();query['queryParameters'][2]['parameterValue']['arrayValues'][0]['value']='999999'
        with self.assertRaises(ValueError):verify_returned_query(query,self.cap,self.expected)

    def test_changed_date_is_rejected(self):
        query=self.query();query['queryParameters'][0]['parameterValue']['value']='2024-01-02'
        with self.assertRaises(ValueError):verify_returned_query(query,self.cap,self.expected)

    def test_changed_empty_array_type_is_rejected(self):
        query=self.query();query['queryParameters'][3]['parameterType']['arrayType']['type']='INT64'
        query['queryParameters'][3].pop('parameterValue')
        with self.assertRaises(ValueError):verify_returned_query(query,self.cap,self.expected)

    def test_changed_cap_or_duplicate_parameter_is_rejected(self):
        query=self.query();query['maximumBytesBilled']=str(self.cap+1)
        with self.assertRaises(ValueError):verify_returned_query(query,self.cap,self.expected)
        query=self.query();query['queryParameters'].append(copy.deepcopy(query['queryParameters'][0]))
        with self.assertRaises(ValueError):verify_returned_query(query,self.cap,self.expected)


if __name__=='__main__':unittest.main()
