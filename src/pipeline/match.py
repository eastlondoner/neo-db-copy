from neo4j import Driver
from threading import Thread
import time

import sys
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--from-host', metavar='fromhost', type=str,
                    help='from host')
parser.add_argument('--to-host', metavar='tohost', type=str,
                    help='to host')

parser.add_argument('--from-user', metavar='N', type=str,
                    default='neo4j',
                    help='from username')
parser.add_argument('--from-password', metavar='N', type=str,
                    help='from password')

parser.add_argument('--to-user', metavar='N', type=str,
                    default='neo4j',
                    help='to username')
parser.add_argument('--to-password', metavar='N', type=str,
                    help='to password')

parser.add_argument('--relationship-key', metavar='N', type=str,
                    default='distance',
                    help='property key that uniquely identifies a relationship')
parser.add_argument('--node-key', metavar='N', type=str,
                    default='name',
                    help='property key that uniquely identifies a node')

args = parser.parse_args(sys.argv[1:])

fromDriver = ("bolt://"+args.from_host, (args.from_user, args.from_password))
toDriver = ("bolt://"+args.to_host, (args.to_user, args.to_password))

relationship_key = args.relationship_key
node_key = args.node_key

in_flight = dict()

def unpack_node(n):
    props = n[2]
    key = props[node_key]
    return key, props


def unpack(response):
    (n, m, r) = response
    n_key, n_props = unpack_node(n)
    m_key, m_props = unpack_node(m)
    r_props = r[4]
    r_key = r_props[relationship_key]
    return (n_key, m_key, r_key), (n_props, m_props, r_props)


# This takes the results of 'MATCH (n)-[r]->(m) RETURN n,m,r' and turns them in to a cypher query
def convert_to_match(response):
    keys, props = unpack(response)
    in_flight[keys] = props
    return f'MATCH (n {{ {node_key}:$n_key }})-[r {{ {relationship_key}:$r_key }}]->(m {{ {node_key}:$m_key }}) RETURN n,m,r', {
        'n_key': keys[0],
        'm_key': keys[1],
        'r_key': keys[2]
    }


# This takes the results of 'MATCH (n)-[r]->(m) RETURN n,m,r' and checks if it's equal to the item in our cache
def evaluate_equality(response):
    keys, target = unpack(response)
    match = in_flight.pop(keys, None)
    if match is None or match != target:
        print(match, target)
        raise Exception("These don't match")


# This takes the results of 'MATCH (n) WHERE NOT ( (n)--() ) RETURN n' and turns them in to a cypher query
def convert_to_match_isolated_node(response):
    (n,) = response
    key, props = unpack_node(n)
    in_flight[key] = props
    return f'MATCH (n {{ {node_key}:$n_key }}) WHERE NOT ( (n)--() ) RETURN n', {
        'n_key': key,
    }


# This takes the results of 'MATCH (n)-[r]->(m) RETURN n,m,r' and checks if it's equal to the item in our cache
def evaluate_equality_isolated_node(response):
    (n,) = response
    keys, target = unpack_node(n)
    match = in_flight.pop(keys, None)
    if match is None or match != target:
        print(match, target)
        raise Exception("These don't match")


def count_nodes(tx):
    result = tx.run("MATCH (n) RETURN count(n)")
    return result.single()[0]


def count_rels(tx):
    result = tx.run("MATCH ()-[r]->() RETURN count(r)")
    return result.single()[0]


def count_isolated_nodes(tx):
    result = tx.run("MATCH (n) WHERE NOT ( (n)--() )  RETURN count(n)")
    return result.single()[0]


class Pusher(Thread):

    def __init__(self, pipeline, source):
        super(Pusher, self).__init__(daemon=True)
        self.pipeline = pipeline
        self.running = True
        self.count = 0
        self.source = source
        self.error = None

    def run(self):
        try:
            i = 0
            for (query, kwargs) in self.source:
                i += 1
                if not self.running:
                    break
                self.pipeline.push(query, kwargs)
                self.count += 1
        except Exception as e:
            self.error = e
            raise


class Puller(Thread):

    def __init__(self, pipeline, sink):
        super(Puller, self).__init__(daemon=True)
        self.pipeline = pipeline
        self.running = True
        self.count = 0
        self.sink = sink

    def run(self):

        while self.running:
            for resp in self.pipeline.pull():
                self.sink(resp)
                self.count += 1
                # print('pulled', self.count)


def flatten_once(gen):
    for batch in gen:
        # print('flattening', batch)
        if batch is None:
            print(f"None batch!?")
        else:
            for i in batch:
                # print(i)
                yield i


# from neobolt.diagnostics import watch
# watch("neobolt")
with Driver(fromDriver[0], auth=fromDriver[1]) as fm:
    t_start = time.time()
    fm_rel_count: int
    to_rel_count: int
    with fm.session() as s:
        fm_node_count = s.read_transaction(count_nodes)
        print("from node count:", fm_node_count)
        fm_isolated_node_count = s.read_transaction(count_isolated_nodes)
        print("from isolated node count:", fm_isolated_node_count)
        fm_rel_count = s.read_transaction(count_rels)
        print("from rel count:", fm_rel_count)

    with Driver(toDriver[0], auth=toDriver[1], encrypted=True) as to:
        with to.session() as s:
            to_node_count = s.read_transaction(count_nodes)
            print("to node count", to_node_count)
            to_isolated_node_count = s.read_transaction(count_isolated_nodes)
            print("to isolated node count:", to_isolated_node_count)
            to_rel_count = s.read_transaction(count_rels)
            print("to rel count", to_rel_count)

        assert fm_rel_count == to_rel_count, "graphs don't have the same number of relationships"
        assert fm_node_count == to_node_count, "graphs don't have the same number of nodes"
        assert fm_isolated_node_count == to_isolated_node_count, "graphs don't have the same number of isolated nodes"

        fm_p = fm.pipeline(flush_every=0)
        fm_pusher = Pusher(fm_p, ((x, None) for x in ['MATCH (n)-[r]->(m) RETURN n,m,r']))
        to_p = to.pipeline(flush_every=0)

        to_pusher = Pusher(to_p, (convert_to_match(patch) for patch in fm_p.pull()))
        to_puller = Puller(to_p, lambda x: evaluate_equality(x))
        try:
            t0 = time.time()
            fm_pusher.start()
            to_pusher.start()
            to_puller.start()
            backlog = 0
            while to_puller.count < fm_rel_count:
                backlog = to_puller.count - to_puller.count
                if fm_pusher.error is not None:
                    raise fm_pusher.error
                if to_pusher.error is not None:
                    raise to_pusher.error
                print("sent %d, received %d, backlog %d, speed %d" % (
                    to_pusher.count, to_puller.count, to_pusher.count - to_puller.count,
                    to_puller.count / float(time.time() - t0)))
                time.sleep(2)
        finally:
            fm_pusher.running = False
            fm_pusher.join(timeout=1)
            print(1)
            to_pusher.running = False
            to_pusher.join(timeout=1)
            print(2)
            to_puller.running = False
            to_puller.join(timeout=1)
            print(3)

        if fm_isolated_node_count > 0:
            fm_p = fm.pipeline(flush_every=0)
            fm_pusher = Pusher(fm_p, ((x, None) for x in ['MATCH (n) WHERE NOT ( (n)--() ) RETURN n']))
            to_p = to.pipeline(flush_every=0)

            to_pusher = Pusher(to_p, (convert_to_match_isolated_node(patch) for patch in fm_p.pull() if patch is not None))
            to_puller = Puller(to_p, lambda x: evaluate_equality_isolated_node(x))
            try:
                t0 = time.time()
                fm_pusher.start()
                to_pusher.start()
                to_puller.start()
                backlog = 0
                while to_puller.count < fm_isolated_node_count:
                    backlog = to_pusher.count - to_puller.count
                    if fm_pusher.error is not None:
                        raise fm_pusher.error
                    if to_pusher.error is not None:
                        raise to_pusher.error
                    print("sent %d, received %d, backlog %d, speed %d" % (
                        to_pusher.count, to_puller.count, to_pusher.count - to_puller.count,
                        to_puller.count / float(time.time() - t0)))
                    time.sleep(2)
            finally:
                fm_pusher.running = False
                fm_pusher.join(timeout=1)
                print(1)
                to_pusher.running = False
                to_pusher.join(timeout=1)
                print(2)
                to_puller.running = False
                to_puller.join(timeout=1)
                print(3)
    print(f"Time elapsed {time.time() - t_start}")