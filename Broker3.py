import zmq
from pubsub_pb2 import Publication
from collections import deque, defaultdict

class Broker3:
    def __init__(self, window_size=3):
        self.context = zmq.Context()
        self.window_size = window_size

        # Socket SUB de la Broker 1 - publicatii
        self.pub_receiver = self.context.socket(zmq.SUB)
        self.pub_receiver.connect("tcp://localhost:5558")
        self.pub_receiver.setsockopt_string(zmq.SUBSCRIBE, "")

        # Socket PULL de la Broker 4 (subscriptii complexe)
        self.sub_receiver = self.context.socket(zmq.PULL)
        self.sub_receiver.bind("tcp://*:5561")

        # Socket PUSH pentru notificari catre Broker4
        self.notif_sender = self.context.socket(zmq.PUSH)
        self.notif_sender.connect("tcp://localhost:5565")

        # Subscriptii complexe
        self.complex_subs = []

        # Ferestre de publicatii pe oras - sliding window
        self.city_temp_windows = defaultdict(lambda: deque(maxlen=self.window_size))

    def calculate_avg(self, pubs, field):
        values = [getattr(p, field, None) for p in pubs]
        values = [float(v) for v in values if v is not None]
        return sum(values) / len(values) if values else None

    def match_avg_temp(self, avg_temp, op, value):
        if op == ">" and avg_temp > value:
            return True
        elif op == ">=" and avg_temp >= value:
            return True
        elif op == "<" and avg_temp < value:
            return True
        elif op == "<=" and avg_temp <= value:
            return True
        elif op == "=" and avg_temp == value:
            return True
        return False

    def run(self):
        poller = zmq.Poller()
        poller.register(self.pub_receiver, zmq.POLLIN)
        poller.register(self.sub_receiver, zmq.POLLIN)

        print("[Broker3] Pornit. Asteapta publicatii si subscriptii complexe...")

        while True:
            socks = dict(poller.poll())

            # Publicatie noua
            if self.pub_receiver in socks:
                msg = self.pub_receiver.recv()
                pub = Publication()
                pub.ParseFromString(msg)
                city = pub.city
                temp = pub.temp

                print(f"[Broker3] Publicatie primita: {city}, Temp: {temp}")

                # Actualizeaza fereastra
                self.city_temp_windows[city].append(temp)
                print(f"[Broker3] Fereastra actualizata pentru {city}: {list(self.city_temp_windows[city])}, lungime={len(self.city_temp_windows[city])}")

                # Verifica subscriptiile doar daca fereastra e plina
                if len(self.city_temp_windows[city]) == self.window_size:
                    avg_temp = sum(self.city_temp_windows[city]) / self.window_size

                    sent_to = set()

                    for sub in self.complex_subs:
                        if sub["subscriber_id"] in sent_to:
                            continue
                        if sub["city"] == city:
                            field, op, value = sub["condition"]

                            if self.match_avg_temp(avg_temp, op, float(value)):
                                notif = {
                                    "subscriber_id": sub["subscriber_id"],
                                    "city": city,
                                    "conditions": True,
                                    "avg_temp": avg_temp,
                                    "timestamp": pub.timestamp
                                }
                                self.notif_sender.send_json(notif)
                                sent_to.add(sub["subscriber_id"])
                                print(
                                    f"[Broker3] Notificare trimisa catre {sub['subscriber_id']} pentru {city}. avg_temp={avg_temp}")

            # Subscriptie noua
            if self.sub_receiver in socks:
                raw = self.sub_receiver.recv_json()
                if raw.get("type") == "window":
                    self.complex_subs.append(raw)
                    print(f"[Broker3] Subscriptie complexa inregistrata: {raw}")

if __name__ == "__main__":
    broker = Broker3(window_size=3)
    broker.run()
