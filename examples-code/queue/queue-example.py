import threading, queue

# q = queue.SimpleQueue()
q = queue.Queue()


def worker():
    while True:
        # get the first item queued. It's blocking
        # until there is an item on queue
        item = q.get()
        print(f'Working on {item}')
        print(f'Finished {item}')
        # remove item from queue when using q = queue.Queue()
        q.task_done()


# turn-on the worker thread
threading.Thread(target=worker, daemon=True).start()

# send ten task requests to the worker
for item in range(10):
    print(f'Queuing task {item}')
    # put item on queue (as the last item)
    q.put(item)
print('All task requests sent\n', end='')
print('Items on queue: ', q.qsize())

# block until all tasks are done, when using q.task_done()
q.join()

