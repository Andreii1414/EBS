import json
import threading
from Subscriber import Subscriber
import time


def distribute_subscriptions(subscribers_count=3):
    """Imparte subscriptiile din fisier in mod egal intre subscriberi"""
    with open("subscriptions.txt", "r") as f:
        all_subs = [json.loads(line.strip()) for line in f if line.strip()]

    # Grupeaza subscriptiile pe baza indexului
    per_subscriber = [[] for _ in range(subscribers_count)]
    for i, sub in enumerate(all_subs):
        per_subscriber[i % subscribers_count].append(sub)

    return per_subscriber


def run_subscriber(subscriber_id, subscriptions):
    """Porneste un subscriber cu lista lui de subscriptii"""
    subscriber = Subscriber(subscriber_id)
    threading.Thread(target=subscriber.listen_for_notifications, daemon=True).start()
    for sub in subscriptions:
        sub["subscriber_id"] = subscriber_id
        subscriber.register_subscription(sub)
        time.sleep(1)
    threading.Thread(target=subscriber.listen_for_notifications, daemon=True).start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    # Imparte subscriptiile in 3 parti egale
    subscriptions_per_subscriber = distribute_subscriptions(subscribers_count=3)

    # Porneste fiecare subscriber cu subscriptiile alocate
    threads = []
    for i in range(3):
        t = threading.Thread(
            target=run_subscriber,
            args=(f"sub{i + 1}", subscriptions_per_subscriber[i])
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()