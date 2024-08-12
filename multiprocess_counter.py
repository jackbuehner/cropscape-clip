import inspect
import multiprocessing
import time
from multiprocessing import Manager
import threading
from contextlib import contextmanager

def watch_counter(shared_counter, lock, callback):
  callback_args_length = len(inspect.signature(callback).parameters)
  
  last_value = shared_counter.value
  try:
    while True:
      with lock:
        current_value = shared_counter.value
      if current_value != last_value:
        if callback_args_length == 1: callback(current_value)
        elif callback_args_length == 2: callback(current_value, last_value)
        last_value = current_value
      time.sleep(1)  # polling interval
  except Exception as e:
    if str(e) == '[Errno 32] Broken pipe': pass
    else:
      print(f'"{e}"')
      print(f'Error in watch_counter: {e}')

@contextmanager
def multiprocess_counter(callback):
  """
  Context manager that provides shared resources for threaded counter.
  Provide the `shared_counter` and `lock` objects to a child thread of process
  using ProcessPoolExecutor. Provide a callback that is executed when the counter
  changes. In the child process, change the counter
  with: `with lock: shared_counter.value += 1`.

  Args:
    callback (function): The callback function to be executed by the watcher thread.
    The first paramater is the current counter value, and the second parameter is
    the previous counter value. The second parameter is optional.

  Yields:
    tuple: A tuple containing the shared counter and lock objects. `(shared_counter, lock)`

  Raises:
    None

  Returns:
    None
  """
  with Manager() as manager:
    shared_counter = manager.Value('i', 0)  # 'i' for integer type, initialized to 0
    lock = manager.Lock()  # create a lock for synchronization

    # start the watcher thread
    watcher_thread = threading.Thread(target=watch_counter, args=(shared_counter, lock, callback))
    watcher_thread.daemon = True
    watcher_thread.start()

    try:
      yield shared_counter, lock
    finally:
      # cleanup if necessary (e.g., stopping the watcher thread if needed)
      pass
