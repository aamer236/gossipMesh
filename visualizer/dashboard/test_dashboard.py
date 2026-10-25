"""Unit and opt-in real-process tests: DASHBOARD_E2E=1 python3 -m unittest visualizer.dashboard.test_dashboard -v"""
import os
import time
import unittest
import threading
import http.client
from visualizer.dashboard.server import make_server
from visualizer.dashboard.runtime import Manager, configuration, correct_ring, percentile

class UnitTests(unittest.TestCase):
    def test_configuration_bounds(self):
        for n in (2, 8, 16, 32, 64):
            self.assertEqual(configuration({'nodes': n})['nodes'], n)
        for data in ({'nodes': 1}, {'nodes': 65}, {'nodes': True}, {'rate': 0}, {'profile': []}, {'extra': 1}):
            with self.assertRaises(ValueError): configuration(data)

    def test_oracle_rejects_missing_live_peers(self):
        self.assertFalse(correct_ring([], [{'id': 0, 'addr': 'a'}]))
        self.assertFalse(correct_ring([{'id': '0', 'addr': 'a'}], [{'id': 1, 'addr': 'b'}]))
        self.assertIsNone(percentile([]))
        self.assertEqual(percentile(range(100)), 94)

class HTTPTests(unittest.TestCase):
    def test_local_controls_and_static_files(self):
        server=make_server(Manager({}),0)
        worker=threading.Thread(target=server.serve_forever,daemon=True)
        worker.start()
        try:
            def request(method,path,body=None,headers=None):
                conn=http.client.HTTPConnection('127.0.0.1',server.server_port)
                conn.request(method,path,body,headers or {})
                response=conn.getresponse()
                result=(response.status,response.read())
                conn.close()
                return result
            self.assertEqual(request('GET','/')[0],200)
            self.assertEqual(request('GET','/../runtime.py')[0],404)
            self.assertEqual(request('POST','/api/start','{}',{'Content-Type':'application/json','Origin':'https://example.com'})[0],403)
            self.assertEqual(request('POST','/api/start','{"nodes":65}',{'Content-Type':'application/json'})[0],400)
            self.assertEqual(request('GET','/api/state',headers={'Host':'attacker.invalid'})[0],403)
            self.assertEqual(request('GET','/api/export')[0],200)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

@unittest.skipUnless(os.environ.get('DASHBOARD_E2E'), 'Set DASHBOARD_E2E=1 after building dashboard binaries')
class ProcessTests(unittest.TestCase):
    def wait_for(self, manager, predicate, timeout=300):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            state=manager.snapshot()
            self.assertNotEqual(state['status'], 'error', state.get('error'))
            if predicate(state): return state
            time.sleep(.2)
        self.fail('Timed out: '+str({k:v['summary'] for k,v in manager.snapshot()['variants'].items()}))

    def test_paired_lifecycle(self):
        manager=Manager({k:'/tmp/gossipmesh-dashboard-bin/'+k for k in ('chord','swim')})
        for count in map(int, os.environ.get("DASHBOARD_TEST_NODES", "2,4,8").split(",")):
            with self.subTest(nodes=count):
                try:
                    manager.start({'nodes':count,'profile':'fast'})
                    self.wait_for(manager,lambda s:s['status']=='running')
                    state=self.wait_for(manager,lambda s:all(v['summary']['samples']>=20 for v in s['variants'].values()))
                    self.assertTrue(all(v['summary']['correct_pct']==100 for v in state['variants'].values()))
                    if count==4:
                        manager.act({'action':'crash','node':128})
                        self.wait_for(manager,lambda s:all(r['seconds'] is not None for r in s['faults'][-1]['recovery'].values()))
                        manager.act({'action':'restart','node':128})
                        self.wait_for(manager,lambda s:all(r['seconds'] is not None for r in s['faults'][-1]['recovery'].values()))
                        manager.act({'action':'delay','node':128,'delay_ms':100})
                        manager.act({'action':'heal','node':128})
                        self.assertIn('samples',manager.snapshot(export=True))
                finally:
                    manager.stop()
                    self.assertTrue(all(n['process'].poll() is not None for nodes in manager.experiment.rings.values() for n in nodes))
                    self.assertFalse(manager.experiment.thread.is_alive())

if __name__=='__main__': unittest.main()
