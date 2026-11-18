"""Test that the local validator detects missing, duplicated and wrong fixtures.

These are synthetic local tests; they are not records of remote execution.
"""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]


class ProbeValidationTests(unittest.TestCase):
    def run_result(self,kind,frame):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'synthetic.csv';frame.to_csv(path,index=False)
            return subprocess.run([sys.executable,str(ROOT/'utils/validate_probe_result.py'),
                                   '--kind',kind,'--csv',str(path)],capture_output=True,text=True).returncode

    def time_result(self):
        d=pd.read_csv(ROOT/'reference_inputs/time_fixture_v01.csv')
        return d[['fixture_id','expected_period','expected_slot10','expected_slot30']].rename(columns={
            'expected_period':'period','expected_slot10':'slot10','expected_slot30':'slot30'})

    def test_correct_time_can_be_reordered(self):
        self.assertEqual(self.run_result('time',self.time_result().iloc[::-1]),0)

    def test_nine_am_is_excluded(self):
        d=self.time_result();d.loc[d.fixture_id.eq('t06'),'period']='AM'
        self.assertNotEqual(self.run_result('time',d),0)

    def test_missing_time_fixture_is_rejected(self):
        self.assertNotEqual(self.run_result('time',self.time_result().iloc[:-1]),0)

    def test_duplicate_spatial_assignment_is_rejected(self):
        d=pd.read_csv(ROOT/'reference_inputs/synthetic_grid_fixture_v01.csv',keep_default_na=False)
        d=d[['fixture_id','expected_grid_id']].rename(columns={'expected_grid_id':'grid_id'})
        self.assertEqual(self.run_result('geo',d),0)
        self.assertNotEqual(self.run_result('geo',pd.concat([d,d.iloc[:1]])),0)

    def test_clipped_outside_point_cannot_be_assigned(self):
        d=pd.read_csv(ROOT/'reference_inputs/synthetic_grid_fixture_v01.csv',keep_default_na=False)
        ix=d.index[d.expected_grid_id.eq('')][0]
        d=d[['fixture_id','expected_grid_id']].rename(columns={'expected_grid_id':'grid_id'})
        d.loc[ix,'grid_id']='sq_0_0'
        self.assertNotEqual(self.run_result('geo',d),0)


if __name__=='__main__':
    unittest.main()
