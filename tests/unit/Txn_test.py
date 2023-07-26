import redis
import threading
import time

def basic_transaction_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.set('user1_balance', 100)
    redis_client.set('user2_balance', 50)

    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.decrby('user1_balance', 30)
        pipeline.incrby('user2_balance', 30)
        pipeline.execute()

    print("基本事务测试成功！")
    print(f"用户1余额: {redis_client.get('user1_balance')}")
    print(f"用户2余额: {redis_client.get('user2_balance')}")

def watch_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.set('user_balance', 100)
    balance_key = 'user_balance'

    # Function for a separate thread to modify the balance while the main thread is watching it
    def modify_balance():
        time.sleep(1)  # Wait for the main thread to start watching
        with redis_client.pipeline(transaction=True) as pipeline:
            pipeline.incrby(balance_key, 50)
            pipeline.execute()

    thread = threading.Thread(target=modify_balance)
    thread.start()

    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.watch(balance_key)
        balance = int(redis_client.get(balance_key))
        pipeline.multi()
        pipeline.decrby(balance_key, 30)
        pipeline.incrby(balance_key, 30)

        time.sleep(2)  # Allow time for the other thread to modify the balance
        try:
            pipeline.execute()
            print("WATCH 功能测试成功！")
            print(f"用户余额: {redis_client.get(balance_key)}")
        except redis.exceptions.WatchError:
            print("WATCH 功能测试失败！发生并发冲突。")

    thread.join()

def concurrent_test():
    def transfer_money(from_account, to_account, amount):
        with redis_client.pipeline(transaction=True) as pipeline:
            pipeline.decrby(from_account, amount)
            pipeline.incrby(to_account, amount)
            pipeline.execute()

    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.set('user1_balance', 100)
    redis_client.set('user2_balance', 50)

    threads = []
    for _ in range(10):
        thread = threading.Thread(target=transfer_money, args=('user1_balance', 'user2_balance', 10))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("并发测试成功！")
    print(f"用户1余额: {redis_client.get('user1_balance')}")
    print(f"用户2余额: {redis_client.get('user2_balance')}")

def nested_transaction_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

    with redis_client.pipeline(transaction=True) as outer_pipeline:
        outer_pipeline.decrby('user1_balance', 30)
        outer_pipeline.incrby('user2_balance', 30)

        with redis_client.pipeline(transaction=True) as inner_pipeline:
            inner_pipeline.decrby('user1_balance', 10)
            inner_pipeline.incrby('user2_balance', 10)
            inner_pipeline.execute()

        outer_pipeline.execute()

    print("嵌套事务测试成功！")
    print(f"用户1余额: {redis_client.get('user1_balance')}")
    print(f"用户2余额: {redis_client.get('user2_balance')}")

def error_handling_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.set('user_balance', 100)

    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.decrby('user_balance', 30)
        pipeline.incrby('non_existent_key', 10)

        try:
            pipeline.execute()
        except redis.exceptions.ResponseError as e:
            print(f"发生错误：{e}")

    print(f"用户余额: {redis_client.get('user_balance')}")

def atomicity_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

    # Multi-key transaction for atomicity
    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.set('key1', 'value1')
        pipeline.set('key2', 'value2')
        pipeline.execute()

        assert redis_client.get('key1') == b'value1'
        assert redis_client.get('key2') == b'value2'

        # One of the set operations fails
        with redis_client.pipeline(transaction=True) as failed_pipeline:
            failed_pipeline.set('key3', 'value3')
            failed_pipeline.set('key4', 'value4')
            failed_pipeline.set('key5', 'value5')
            failed_pipeline.execute()

            assert redis_client.get('key3') is None
            assert redis_client.get('key4') is None
            assert redis_client.get('key5') is None

    print("原子性测试成功！")

def consistency_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

    # Use a single key to demonstrate consistency
    key = 'test_key'
    redis_client.set(key, 'initial_value')

    def modify_value():
        time.sleep(1)  # Wait for the main thread to start watching
        with redis_client.pipeline(transaction=True) as pipeline:
            pipeline.set(key, 'modified_value')
            pipeline.execute()

    thread = threading.Thread(target=modify_value)
    thread.start()

    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.watch(key)
        value = redis_client.get(key)
        pipeline.multi()
        pipeline.set(key, 'new_value')

        time.sleep(2)  # Allow time for the other thread to modify the value
        pipeline.execute()

    thread.join()

    # The value should remain 'initial_value' as the watch prevented the modification
    assert redis_client.get(key) == b'initial_value'

    print("一致性测试成功！")

def isolation_test():
    # Use separate databases for isolation test
    redis_client_db1 = redis.StrictRedis(host='localhost', port=6379, db=1)
    redis_client_db2 = redis.StrictRedis(host='localhost', port=6379, db=2)

    # Set initial values in separate databases
    redis_client_db1.set('key', 'value1')
    redis_client_db2.set('key', 'value2')

    # Perform transactions in separate threads
    def transaction_in_db1():
        with redis_client_db1.pipeline(transaction=True) as pipeline:
            pipeline.get('key')
            time.sleep(1)  # Wait for the other thread to start
            pipeline.set('key', 'new_value1')
            pipeline.execute()

    def transaction_in_db2():
        with redis_client_db2.pipeline(transaction=True) as pipeline:
            pipeline.get('key')
            pipeline.set('key', 'new_value2')
            pipeline.execute()

    thread1 = threading.Thread(target=transaction_in_db1)
    thread2 = threading.Thread(target=transaction_in_db2)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    # In a fully isolated transaction, values in each database should not be affected by the other
    assert redis_client_db1.get('key') == b'new_value1'
    assert redis_client_db2.get('key') == b'new_value2'

    print("隔离性测试成功！")

def durability_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.set('key', 'value')

    # Simulate crash after a transaction
    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.set('key', 'new_value')
        pipeline.execute()

    # Redis data should be persisted even after a crash
    # Restart Redis here if you want to demonstrate data persistence across Redis restarts
    # redis-server --dir /path/to/redis/data/directory
    assert redis_client.get('key') == b'new_value'

    print("持久性测试成功！")

def flush_in_transaction_test():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.set('key', 'value')
    redis_client.set('key_in_db1', 'value_in_db1')
    redis_client.set('key_in_db2', 'value_in_db2')

    with redis_client.pipeline(transaction=True) as pipeline:
        pipeline.get('key')
        pipeline.flushdb()  # FLUSHDB within the transaction
        pipeline.get('key_in_db1')
        pipeline.get('key_in_db2')
        pipeline.flushall()  # FLUSHALL within the transaction
        pipeline.get('key')
        pipeline.get('key_in_db1')
        pipeline.get('key_in_db2')
        result = pipeline.execute()

    print("Flush in Transaction Test Result:")
    print(f"Get 'key': {result[0]}")  # Get 'key': b'value'
    print(f"Get 'key_in_db1': {result[1]}")  # Get 'key_in_db1': None
    print(f"Get 'key_in_db2': {result[2]}")  # Get 'key_in_db2': None
    print(f"Get 'key': {result[3]}")  # Get 'key': b'value'
    print(f"Get 'key_in_db1': {result[4]}")  # Get 'key_in_db1': b'value_in_db1'
    print(f"Get 'key_in_db2': {result[5]}")  # Get 'key_in_db2': b'value_in_db2'

if __name__ == "__main__":
    basic_transaction_test()
    # watch_test()
    concurrent_test()
    nested_transaction_test()
    error_handling_test()
    # atomicity_test()
    # consistency_test()
    # isolation_test()
    # durability_test()
    # flush_in_transaction_test()
