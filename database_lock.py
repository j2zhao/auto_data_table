import portalocker
import time
import os
from typing import Optional

class MultiLock:
    """
    A multi-lock allowing multiple readers or one writer at a time,
    with optional timeout support.
    """
    def __init__(self, lock_file):
        """
        Initializes the ReadWriteLock with the specified lock file.
        
        :param lock_file: Path to the lock file used for synchronization.
        """
        self.lock_file = lock_file
        self.read_handle = None
        self.write_handle = None

        # Ensure the lock file exists
        if not os.path.exists(self.lock_file):
            open(self.lock_file, 'a').close()

    def acquire_shared(self, timeout=None, check_interval=0.1):
        """
        Acquires a shared (read) lock.

        :param timeout: Maximum time (in seconds) to wait for the lock. None means wait indefinitely.
        :param check_interval: Time (in seconds) between lock acquisition attempts.
        :return: True if the lock was acquired, False otherwise.
        :raises TimeoutError: If the lock could not be acquired within the timeout.
        """
        start_time = time.time()
        while True:
            try:
                # Open the lock file in read mode
                self.read_handle = open(self.lock_file, 'r')
                # Attempt to acquire a shared lock without blocking
                portalocker.lock(self.read_handle, portalocker.LOCK_SH | portalocker.LOCK_NB)
                return True  # Lock acquired
            except (portalocker.exceptions.LockException, IOError):
                # Failed to acquire lock
                if self.read_handle:
                    self.read_handle.close()
                    self.read_handle = None
                if timeout is not None and (time.time() - start_time) >= timeout:
                    raise TimeoutError("Timeout while trying to acquire read lock.")
                time.sleep(check_interval)

    def release_shared(self):
        """
        Releases the shared (read) lock.
        """
        if self.read_handle:
            try:
                portalocker.unlock(self.read_handle)
            finally:
                self.read_handle.close()
                self.read_handle = None

    def acquire_exclusive(self, timeout=None, check_interval=0.1):
        """
        Acquires an exclusive (write) lock.

        :param timeout: Maximum time (in seconds) to wait for the lock. None means wait indefinitely.
        :param check_interval: Time (in seconds) between lock acquisition attempts.
        :return: True if the lock was acquired, False otherwise.
        :raises TimeoutError: If the lock could not be acquired within the timeout.
        """
        start_time = time.time()
        while True:
            try:
                # Open the lock file in append mode to ensure it exists
                self.write_handle = open(self.lock_file, 'a+')
                # Attempt to acquire an exclusive lock without blocking
                portalocker.lock(self.write_handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
                return True  # Lock acquired
            except (portalocker.exceptions.LockException, IOError):
                # Failed to acquire lock
                if self.write_handle:
                    self.write_handle.close()
                    self.write_handle = None
                if timeout is not None and (time.time() - start_time) >= timeout:
                    raise TimeoutError("Timeout while trying to acquire write lock.")
                time.sleep(check_interval)

    def release_exclusive(self):
        """
        Releases the exclusive (write) lock.
        """
        if self.write_handle:
            try:
                portalocker.unlock(self.write_handle)
            finally:
                self.write_handle.close()
                self.write_handle = None

    def __del__(self):
        """
        Destructor to ensure that any held locks are released.
        """
        self.release_shared()
        self.release_exclusive()


class DatabaseLock():
    def __init__(self, db_dir: str,  table_name:Optional[str] = None, table_id:Optional[str] = None):
        db_lock_dir = os.path.join(db_dir, 'locks', 'DATABASE.lock')
        self.db_lock = MultiLock(db_lock_dir)
        if table_name:
            
            table_lock_dir = os.path.join(db_dir, 'locks', table_name)
            if not os.path.exists(table_lock_dir):
                os.mkdir(table_lock_dir)
            table_lock_dir = os.path.join(table_lock_dir, 'TABLE.lock')
            self.table_lock = MultiLock(table_lock_dir)
        if table_id:
            instance_lock_dir = os.path.join(db_dir, 'locks', table_name, f'{table_id}.lock')
            self.instance_lock = MultiLock(instance_lock_dir)
        self.table_id = table_id
        self.table_name = table_name

    def acquire_shared_lock(self, timeout=None, check_interval=0.1):
        try:
            if self.table_id:
                self.db_lock.acquire_shared(timeout, check_interval)
                self.table_lock.acquire_shared(timeout, check_interval)
                self.instance_lock.acquire_shared(timeout, check_interval)
            elif self.table_name:
                self.db_lock.acquire_shared(timeout, check_interval)
                self.table_lock.acquire_shared(timeout, check_interval)
            else:
                self.db_lock.acquire_shared(timeout, check_interval)
        except Exception as e:
            self.release_shared_lock()
            raise e

    def acquire_exclusive_lock(self, timeout=None, check_interval=0.1):
        try:
            if self.table_id:
                self.db_lock.acquire_shared(timeout, check_interval)
                self.table_lock.acquire_shared(timeout, check_interval)
                self.instance_lock.acquire_exclusive(timeout, check_interval)
            elif self.table_name:
                self.db_lock.acquire_shared(timeout, check_interval)
                self.table_lock.acquire_exclusive(timeout, check_interval)
            else:
                self.db_lock.acquire_exclusive(timeout, check_interval)
        except Exception as e:
            self.release_exclusive_lock()
            raise e

    def release_shared_lock(self):
        if self.table_id:
            self.instance_lock.release_shared()
            self.table_lock.release_shared()
            self.db_lock.release_shared()
        elif self.table_name:
            self.table_lock.release_shared()
            self.db_lock.release_shared()
        else:
            self.db_lock.release_shared()

    def release_exclusive_lock(self):
        if self.table_id:
            self.instance_lock.release_exclusive()
            self.table_lock.release_shared()
            self.db_lock.release_shared()
        elif self.table_name:
            self.table_lock.release_exclusive()
            self.db_lock.release_shared()
        else:
            self.db_lock.release_exclusive()




