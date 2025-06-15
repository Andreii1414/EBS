import zmq
import json
import time
from pubsub_pb2 import Publication
import datetime


class Subscriber:
    def __init__(self, subscriber_id, broker_host="localhost"):
        self.id = subscriber_id
        self.context = zmq.Context()

        # Socket pentru inregistrarea subscriptiilor
        self.registration_socket = self.context.socket(zmq.PUSH)
        self.registration_socket.connect(f"tcp://{broker_host}:5559")

        # Socket SUB pentru notificari (de la Broker4)
        self.notification_socket = self.context.socket(zmq.SUB)
        self.notification_socket.connect(f"tcp://{broker_host}:5570")  # Broker4 publica aici

        # Asculta doar notificarile care incep cu subscriber_id-ul lui - self.id
        self.notification_socket.setsockopt_string(zmq.SUBSCRIBE, self.id)

    def register_subscription(self, subscription_data):
        """Trimite o subscriptie la Broker4"""
        subscription_data["subscriber_id"] = self.id
        self.registration_socket.send_json(subscription_data)
        print(f"[Subscriber {self.id}] inregistrat: {subscription_data}")

    def listen_for_notifications(self, duration_minutes=3):
        """Asculta notificari de la brokeri"""
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(minutes=duration_minutes)
        count = 0
        latencies = []
        print(f"[Subscriber {self.id}] Asculta notificari timp de {duration_minutes} minute...\n")
        while datetime.datetime.now() < end_time:
            try:
                raw_msg = self.notification_socket.recv_string()
                _, msg_json = raw_msg.split(" ", 1)
                notif = json.loads(msg_json)
                count += 1

                if "timestamp" in notif:
                    recv_time = int(time.time() * 1000)
                    latency = recv_time - int(notif["timestamp"])
                    latencies.append(latency)

                print(f"\n[Subscriber {self.id}] Notificare primita:")

                if notif.get("conditions"):
                    print(
                        f"[META] Oras: {notif['city']} - Conditii indeplinite in fereastra! "
                        f"avg_temp: {notif.get('avg_temp')}")
                else:
                    print(
                        f"Oras: {notif['city']}, Temp: {notif['temp']}°C, "
                        f"Vant: {notif['wind']}km/h, Directie: {notif['direction']}, Data: {notif['date']}")
            except Exception as e:
                print(f"[Subscriber {self.id}] Eroare: {e}")
                time.sleep(1)
        print(f"\n[Subscriber {self.id}] A primit {count} notificari in {duration_minutes} minute.")

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            print(f"[Subscriber {self.id}] Latenta medie: {avg_latency:.2f} ms")
        else:
            print(f"[Subscriber {self.id}] Nu s-au putut calcula latente (lipsa timestamp-uri).")

if __name__ == "__main__":
    subscriber = Subscriber("sub1")
    subscriber.register_subscription({
        "type": "simple",
        "conditions": [["city", "=", "Bucharest"], ["temp", ">", 20]]
    })
    subscriber.listen_for_notifications()