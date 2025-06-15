import zmq
import time
import re
from pubsub_pb2 import Publication

class Publisher:
    def __init__(self, file_path="publications.txt"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)  # Socket PUB pentru distributie
        self.socket.bind("tcp://*:5556")  # Portul pe care asculta brokerul 1
        self.publications = self.load_publications(file_path)

    def parse_publication_line(self, line):
        """Parseaza o linie din fisier si intoarce un dictionar cu datele"""
        pattern = r'"([^"]+)":\s*([^,)]+)'
        matches = re.findall(pattern, line)
        pub_data = {}
        for key, val in matches:
            key = key.strip()
            val = val.strip('"')
            pub_data[key] = val
        return pub_data

    def load_publications(self, file_path):
        """Icarca si parseaza publicatiile din fisier"""
        publications = []
        with open(file_path, "r") as file:
            for line in file:
                line = line.strip()
                if line and line.startswith("{") and line.endswith("}"):
                    pub_data = self.parse_publication_line(line)
                    pub = Publication()
                    pub.station_id = int(pub_data.get("stationid", 0))
                    pub.city = pub_data.get("city", "")
                    pub.temp = float(pub_data.get("temp", 0))
                    pub.rain = float(pub_data.get("rain", 0))
                    pub.wind = float(pub_data.get("wind", 0))
                    pub.direction = pub_data.get("direction", "")
                    pub.date = pub_data.get("date", "")
                    publications.append(pub)
        return publications

    def send_publications(self, delay=1, max_minutes = 3):
        """Trimite publicatiile una cate una cu un delay"""
        start_time = time.time()
        count = 0
        for pub in self.publications:
            elapsed = time.time() - start_time
            if elapsed > max_minutes * 60:
                break
            print(f"[Publisher] Trimite: {pub.city} | Temp: {pub.temp} | Rain: {pub.rain} | Wind: {pub.wind} | Direction: {pub.direction} | Date: {pub.date}")
            pub.timestamp = int(time.time() * 1000)
            self.socket.send(pub.SerializeToString())  # Serializeaza si trimite
            count += 1
            time.sleep(delay)
        print(f"[Publisher] Total publicatii trimise in {max_minutes} minute: {count}")


if __name__ == "__main__":
    publisher = Publisher("publications.txt")
    print(f"Incepe transmiterea a {len(publisher.publications)} publicatii...")
    publisher.send_publications(delay=1)