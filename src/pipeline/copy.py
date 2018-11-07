from neo4j import GraphDatabase, Driver, Session
from queue import Queue
from threading import Thread
import time

if __name__ == "__main__":
    fromDriver = ("bolt://localhost", ("neo4j", "onepassword"))
    toDriver = ("bolt://cbb0ec40.databases.neo4j.io", ("neo4j", "anotherpassword"))


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
                    i+=1
                    if not self.running:
                        break
                    #print(f"pushing {query}", kwargs)
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
                    #print('pulled', self.count)


    def flatten_once(gen):
        for batch in gen:
            #print('flattening', batch)
            if batch is None:
                print(f"None batch!?")
            else:
                for i in batch:
                    #print(i)
                    yield i

    # from neobolt.diagnostics import watch
    # watch("neobolt")
    with Driver(fromDriver[0], auth=fromDriver[1]) as fm:
        with Driver(toDriver[0], auth=toDriver[1], encrypted=True) as to:
            def get_gen(self):
                while self.running:
                    yield self.queue.get()


            fm_p = fm.pipeline(flush_every=0)
            fm_pusher = Pusher(fm_p, ((x, None) for x in [
                'CALL apoc.export.cypher.all(null, {format:"plain",streamStatements:true,batchSize:5000}) YIELD cypherStatements RETURN cypherStatements']))
            to_p = to.pipeline(flush_every=0)
            to_p.push("MATCH (n) DETACH DELETE n")
            for x in to_p.pull():
                print("delete", x)

            to_p._flush_every= 10000
            to_pusher = Pusher(to_p, ((x, None) for x in flatten_once((batch.split(';\n')) for batch in flatten_once(fm_p.pull())) if x.strip() != ''))
            to_puller = Puller(to_p, lambda x: None)
            try:
                t0 = time.time()
                fm_pusher.start()
                to_pusher.start()
                to_puller.start()
                while True:
                    if fm_pusher.error is not None:
                        raise fm_pusher.error
                    if to_pusher.error is not None:
                        raise to_pusher.error
                    print("sent %d, received %d, backlog %d, speed %d" % (to_pusher.count, to_puller.count, to_pusher.count - to_puller.count, to_puller.count/(time.time() - t0)))
                    time.sleep(5)
            except KeyboardInterrupt:
                fm_pusher.running = False
                fm_pusher.join(timeout=1)
                print(1)
                to_pusher.running = False
                to_pusher.join(timeout=1)
                print(2)
                to_puller.running = False
                to_puller.join(timeout=1)
                print(3)
