import zmq
from pubsub_pb2 import Publication
import time

class Broker1:
    def __init__(self):
        self.context = zmq.Context()

        # Socket pentru a primi publicatii de la Publisher
        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.connect("tcp://localhost:5556")
        self.receiver.setsockopt_string(zmq.SUBSCRIBE, "")  # primeste tot

        # Socket pentru forward catre Broker 2
        self.sender_b2 = self.context.socket(zmq.PUB)
        self.sender_b2.bind("tcp://*:5557")

        # Socket pentru forward catre Broker 3
        self.sender_b3 = self.context.socket(zmq.PUB)
        self.sender_b3.bind("tcp://*:5558")

    def run(self):
        print("[Broker1] Pornit. Asteapta publicatii...")
        while True:
            try:
                msg = self.receiver.recv()

                pub = Publication()
                pub.ParseFromString(msg)
                print(f"[Broker1] Primit: {pub.city}, Temp: {pub.temp}, Wind: {pub.wind}, Direction: {pub.direction}, Date: {pub.date}, Station ID: {pub.station_id}, Timestamp: {pub.timestamp}")

                # Forward catre ambii brokeri
                self.sender_b2.send(msg)
                self.sender_b3.send(msg)

            except Exception as e:
                print(f"[Broker1] Eroare: {e}")
                time.sleep(1)

if __name__ == "__main__":
    broker = Broker1()
    broker.run()
