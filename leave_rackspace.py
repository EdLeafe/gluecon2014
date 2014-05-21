#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import datetime
import os
import Queue
import random
import sys
import threading
import time

import pyrax
import pyrax.exceptions as exc
import pyrax.utils as utils

CHUNKSIZE = 65536


def logit(*args):
    """
    Print out a timestamped message. Multiple arguments will be stringified and
    joined with a space.
    """
    msg = " ".join(["%s" % arg for arg in args])
    tm = datetime.datetime.now().strftime("%H:%M:%S -")
    print(tm, msg)


def dlo_upload(ctx, region, cont, parts, base_name, chunk_size=None,
        max_threads=None):
    """
    Uploads a DLO (Dynamic Large Object) that has been fetched into a list of
    generator objects (typically by dlo_fetch) to the specified container.

    It uses a default chunk_size of 64K; you may pass in a different value if
    it improves performance.

    By default this creates a thread for each 'part'. If you wish to reduce
    that for any reason, pass a limiting value to 'max_threads'.
    """
    DEFAULT_CHUNKSIZE = 65536
    if chunk_size is None:
        chunk_size = DEFAULT_CHUNKSIZE

    class Uploader(threading.Thread):
        def __init__(self, client, cont, queue, num):
            super(Uploader, self).__init__()
            self.client = client
            logit("Uploader", num, "created.")
            self.cont = cont
            self.queue = queue
            self.num = num

        def run(self):
            while True:
                try:
                    job = self.queue.get(False)
                    nm, gen = job
                    logit("Uploader #%s storing '%s'." % (self.num, nm))
                    self.client.store_object(self.cont, nm, gen,
                            chunk_size=chunk_size, return_none=True)
                    logit("Uploader #%s finished storing." % self.num)
                except Queue.Empty:
                    break
            logit("**DONE** Uploader #%s terminating." % self.num)

    queue = Queue.Queue()
    for part in parts:
        queue.put(part)
    num_threads = len(parts)
    if max_threads is not None:
        num_threads = min(num_threads, max_threads)

    workers = []
    for num in range(num_threads):
        clt = ctx.get_client("object_store", region, cached=False)
        worker = Uploader(clt, cont, queue, num)
        workers.append(worker)
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    # Upload the manifest
    headers = {"X-Object-Manifest": "%s/%s" % (cont, base_name)}
    clt = ctx.get_client("object_store", region, cached=False)
    clt.store_object(cont, base_name, "", headers=headers, return_none=True)



if __name__ == "__main__":
    rs = pyrax.create_context(env="rackspace")
    rs.keyring_auth()
    hp = pyrax.create_context(env="hp")
    hp.keyring_auth()

    rs_region = "ORD"
    hp_region = "region-a.geo-1"
    rs_compute = rs.get_client("compute", rs_region)
    rs_obj = rs.get_client("object_store", rs_region, public=False)
    rs_image = rs.get_client("image", rs_region)
    hp_compute = hp.get_client("compute", hp_region)
    hp_obj = hp.get_client("object_store", hp_region)
    hp_image = hp.get_client("image", hp_region)

    rs_cont_name = "export_images"
    hp_cont_name = "upload_images"
    instance = rs_compute.servers.find(name="glue_server")
    # Create the snapshot of the server
    snap = instance.create_image(instance, "glue_snap")
    logit("Snapshot created; id =", snap.id)
    utils.wait_for_build(snap, verbose=True)
    # Export it
    task = rs_image.export_task(snap, rs_cont_name)
    logit("Export Task created; id =", task.id)
    utils.wait_for_build(task, verbose=True, desired=["success", "failure"])

    # Transfer it to HP
    obj_name = "%s.vhd" % snap.id
    start = time.time()
    dlo_parts = rs_obj.dlo_fetch(rs_cont_name, obj_name)
    logit("Number of DLO parts:", len(dlo_parts))
    dlo_upload(hp, hp_region, hp_cont_name, dlo_parts, obj_name)
    end = time.time()
    elapsed = end - start
    logit("It took %8.2f seconds to transfer the image." % elapsed)

    # Import the image
    data = hp_obj.fetch(hp_cont_name, obj_name)
    new_image = hp_image.create("glue_image", data=data)

    # Create the new instance
    hp_instance = hp_compute.servers.create("hp_glue_server",
        image=new_image, flavor=some_flavor)
    utils.wait_for_build(hp_instance, verbose=True)
    logit("New Instance Networks:", hp_instance.networks)
