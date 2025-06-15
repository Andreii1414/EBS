import zmq
import json

class Broker4:
    def __init__(self):
        self.context = zmq.Context()

        # Socket PULL de la subscribers
        self.sub_receiver = self.context.socket(zmq.PULL)
        self.sub_receiver.bind("tcp://localhost:5559")

        # Socket PUSH catre Broker 2 (subscriptii simple)
        self.simple_sender = self.context.socket(zmq.PUSH)
        self.simple_sender.connect("tcp://localhost:5560")

        # Socket PUSH catre Broker 3 (subscriptii complexe)
        self.complex_sender = self.context.socket(zmq.PUSH)
        self.complex_sender.connect("tcp://localhost:5561")

        # Socket PULL pentru notificari de la Broker2/Broker3
        self.notif_receiver = self.context.socket(zmq.PULL)
        self.notif_receiver.bind("tcp://*:5564")  # Broker2 notifica aici
        self.notif_receiver.bind("tcp://*:5565")  # Broker3 notifica aici

        # Socket PUB catre subscriberi (notificari finale)
        self.notif_sender = self.context.socket(zmq.PUB)
        self.notif_sender.bind("tcp://*:5570")  # Subscriberii asculta aici

    def run(self):
        poller = zmq.Poller()
        poller.register(self.sub_receiver, zmq.POLLIN)
        poller.register(self.notif_receiver, zmq.POLLIN)

        print("[Broker4] Pornit. Asteapta subscriptii si notificari...")

        while True:
            socks = dict(poller.poll())

            # Proceseaza subscriptii noi
            if self.sub_receiver in socks:
                data = json.loads(self.sub_receiver.recv_string())
                if "conditions" in data:
                    self.simple_sender.send_json(data)
                    print(f"[Broker4] Trimis la Broker2: {data}")
                else:
                    self.complex_sender.send_json(data)
                    print(f"[Broker4] Trimis la Broker3: {data}")

            # Proceseaza notificari de la Broker2/Broker3
            if self.notif_receiver in socks:
                notif = self.notif_receiver.recv_json()
                subscriber_id = notif["subscriber_id"]
                self.notif_sender.send_string(f"{subscriber_id} {json.dumps(notif)}")
                print(f"[Broker4] Notificare trimisa la {subscriber_id}: {notif}")


if __name__ == "__main__":
    broker = Broker4()
    broker.run()
