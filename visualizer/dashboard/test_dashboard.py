"""Unit and opt-in real-process tests: DASHBOARD_E2E=1 python3 -m unittest visualizer.dashboard.test_dashboard -v"""
import os
import time
import unittest
import threading
import http.client
from visualizer.dashboard.server import make_server
from visualizer.dashboard.runtime import Experiment, Manager, configuration, correct_ring, percentile

class UnitTests(unittest.TestCase):
    def test_crash_evidence_is_bounded_and_closed_on_restart(self):
        from collections import deque
        experiment = Experiment.__new__(Experiment)
        experiment.lock = threading.RLock()
        experiment.epoch = time.monotonic()
        experiment.event_seq = 0
        experiment.events = deque(maxlen=240)
        fault = {"type":"crash", "node":128, "evidence":[]}
        experiment.faults = [fault]
        for _ in range(300):
            experiment._event("finger_repaired", "swim", "128", {"local_id":"64"})
        self.assertEqual(len(fault["evidence"]),256)
        experiment._event("peer_dead", "swim", "192")
        self.assertEqual(len(fault["evidence"]),256)
        fault["evidence_closed"] = True
        experiment._event("peer_dead", "swim", "128")
        self.assertEqual(fault["evidence"][-1]["type"],"finger_repaired")

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
                    seeded=state['initial_storage']
                    self.assertTrue(seeded['complete'])
                    expected={str(r['key']) for r in seeded['records']}
                    self.assertGreaterEqual(len(expected),16)
                    for variant in ('chord','swim'):
                        actual={e['key'] for n in state['variants'][variant]['nodes'] for e in n['storage']['entries']}
                        self.assertEqual(actual,expected)
                        for row in seeded['records']:
                            self.assertEqual(row['results'][variant]['value'],row['sent_value'])
                    manager.storage({'operation':'put','key':80,'value':'paired payload','initiator':0})
                    manager.storage({'operation':'get','key':80,'initiator':manager.snapshot()['node_ids'][-1]})
                    stored=manager.snapshot()['storage_operations'][-1]
                    self.assertEqual(stored['key'],80)
                    for result in stored['results'].values():
                        self.assertEqual(result['http_status'],200)
                        self.assertEqual(result['key'],'80')
                        self.assertEqual(result['value'],'paired payload')
                        self.assertTrue(result['found'])
                    if count==4:
                        manager.act({'action':'crash','node':128})
                        self.wait_for(manager,lambda s:all(r['seconds'] is not None for r in s['faults'][-1]['recovery'].values()))
                        trace = manager.snapshot()['faults'][-1]
                        self.assertTrue(trace['before']['swim'])
                        kinds = {event['type'] for event in trace['evidence']}
                        self.assertIn('peer_dead', kinds)
                        self.assertIn('membership_event_handled', kinds)
                        self.assertIn('references_identified', kinds)
                        # Every attributed finger repair provides the actual observer,
                        # index/start and validated replacement owner.
                        for event in trace['evidence']:
                            if event['type'] == 'finger_repaired':
                                self.assertTrue({'local_id','index','start','owner'} <= event['detail'].keys())
                        manager.act({'action':'restart','node':128})
                        self.wait_for(manager,lambda s:all(r['seconds'] is not None for r in s['faults'][-1]['recovery'].values()))
                        manager.storage({'operation':'get','key':80,'initiator':0})
                        for result in manager.snapshot()['storage_operations'][-1]['results'].values():
                            self.assertEqual(result['http_status'],404)
                            self.assertFalse(result['found'])
                        manager.act({'action':'delay','node':128,'delay_ms':100})
                        manager.act({'action':'heal','node':128})
                        self.assertIn('samples',manager.snapshot(export=True))
                finally:
                    manager.stop()
                    self.assertTrue(all(n['process'].poll() is not None for nodes in manager.experiment.rings.values() for n in nodes))
                    self.assertFalse(manager.experiment.thread.is_alive())

if __name__=='__main__': unittest.main()
