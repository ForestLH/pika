import redis
import threading
import time

def watch_test():
    # Create two independent Redis clients to simulate two different users/clients
    redis_client1 = redis.StrictRedis(host='localhost', port=9221, db=0)
    redis_client2 = redis.StrictRedis(host='localhost', port=9221, db=0)

    # Set an initial balance for a user
    user_balance_key = 'user_balance'
    redis_client1.set(user_balance_key, 100)

    # Function to modify the balance in one client while the other is watching
    def modify_balance():
        time.sleep(1)  # Wait for the main thread to start watching
        with redis_client2.pipeline(transaction=True) as pipeline:
            pipeline.decrby(user_balance_key, 50)
            pipeline.execute()

    thread = threading.Thread(target=modify_balance)
    thread.start()

    try:
        # Watch the user balance key using redis_client1
        redis_client1.watch(user_balance_key)

        # Get the current balance
        balance = int(redis_client1.get(user_balance_key))

        # Check if the balance is None (key doesn't exist)
        if balance is None:
            print(f"无法找到 {user_balance_key} 键。")
            return

        # Attempt to increment the balance
        with redis_client1.pipeline(transaction=True) as pipeline:
            pipeline.multi()
            pipeline.incrby(user_balance_key, 30)

            # Simulate processing time to allow the other thread to modify the balance
            time.sleep(2)

            # Execute the transaction
            pipeline.execute()
            print("WATCH 功能测试成功！")
            print(f"用户余额: {redis_client1.get(user_balance_key)}")
    except redis.exceptions.WatchError:
        print("WATCH 功能测试失败！发生并发冲突。")

    thread.join()

if __name__ == "__main__":
    watch_test()
