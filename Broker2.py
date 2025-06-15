import zmq
from pubsub_pb2 import Publication

class Broker2:
    def __init__(self):
        self.context = zmq.Context()

        # Socket SUB de la Broker 1 - publicatii
        self.pub_receiver = self.context.socket(zmq.SUB)
        self.pub_receiver.connect("tcp://localhost:5557")
        self.pub_receiver.setsockopt_string(zmq.SUBSCRIBE, "")

        # Socket PULL de la Broker 4 (subscriptii)
        self.sub_receiver = self.context.socket(zmq.PULL)
        self.sub_receiver.bind("tcp://*:5560")

        # Socket PUSH pentru notificari catre Broker4
        self.notif_sender = self.context.socket(zmq.PUSH)
        self.notif_sender.connect("tcp://localhost:5564")

        # Subscriptiile simple stocate
        self.simple_subs = []

    def match(self, pub, conditions):
        for cond in conditions:
            field, op, value = cond

            if field == "stationid":
                field = "station_id"

            pub_val = getattr(pub, field, None)
            if pub_val is None:
                return False

            if field in ["temp", "wind", "rain", "station_id"]:
                value = float(value)
                pub_val = float(pub_val)

            if op == "=" and pub_val != value:
                return False
            elif op == ">" and not (pub_val > value):
                return False
            elif op == ">=" and not (pub_val >= value):
                return False
            elif op == "<" and not (pub_val < value):
                return False
            elif op == "<=" and not (pub_val <= value):
                return False
        return True

    def run(self):
        poller = zmq.Poller()
        poller.register(self.pub_receiver, zmq.POLLIN)
        poller.register(self.sub_receiver, zmq.POLLIN)

        print("[Broker2] Pornit. Asteapta publicatii si subscriptii...")

        while True:
            socks = dict(poller.poll())

            # Publicatie noua
            if self.pub_receiver in socks:
                msg = self.pub_receiver.recv()
                pub = Publication()
                pub.ParseFromString(msg)
                print(f"[Broker2] Publicatie: {pub.city}, Temp: {pub.temp}, Wind: {pub.wind}, Direction: {pub.direction}, Date: {pub.date}, Station ID: {pub.station_id}")

                sent_to = set()

                for sub in self.simple_subs:
                    if sub["subscriber_id"] in sent_to:
                        continue
                    if self.match(pub, sub["conditions"]):
                        notif = {
                            "subscriber_id": sub["subscriber_id"],
                            "city": pub.city,
                            "temp": pub.temp,
                            "wind": pub.wind,
                            "direction": pub.direction,
                            "date": pub.date,
                            "timestamp": pub.timestamp
                        }
                        self.notif_sender.send_json(notif)
                        sent_to.add(sub["subscriber_id"])
                        print(f"[Broker2] Match pentru {sub['subscriber_id']} -> notificare trimisa. Conditii: {sub['conditions']}")

            # Subscriptie noua
            if self.sub_receiver in socks:
                raw = self.sub_receiver.recv_json()
                if raw.get("type") == "simple":
                    self.simple_subs.append(raw)
                    print(f"[Broker2] Subscriptie inregistrata: {raw}")

if __name__ == "__main__":
    broker = Broker2()
    broker.run()
