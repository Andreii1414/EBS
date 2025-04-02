import random
import time
import json
from multiprocessing import Pool
from functools import partial

STATION_IDS = list(range(1, 101))
CITIES = ["Bucharest", "Cluj", "Timisoara", "Iasi", "Constanta"]
DIRECTIONS = ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]
DATES = ["1.02.2023", "2.02.2023", "3.02.2023", "4.02.2023"]

TEMP_RANGE = (0, 40)
RAIN_RANGE = (0.0, 5.0)
WIND_RANGE = (0, 20)
OPERATORS = ["=", "<", "<=", ">", ">="]

def generate_publication():
    return {
        "stationid": random.choice(STATION_IDS),
        "city": random.choice(CITIES),
        "temp": random.randint(*TEMP_RANGE),
        "rain": round(random.uniform(*RAIN_RANGE), 2),
        "wind": random.randint(*WIND_RANGE),
        "direction": random.choice(DIRECTIONS),
        "date": random.choice(DATES),
    }

def generate_condition(field, num_total, eq_count):
    operators = OPERATORS[1:]

    eq_indices = set(random.sample(range(num_total), eq_count))
    max_eq_count = min(num_total, int(eq_count * 1.3))
    extra_eq_count = random.randint(0, max_eq_count - eq_count)
    extra_indices = set(random.sample(list(set(range(num_total)) - eq_indices), extra_eq_count))
    eq_indices.update(extra_indices)

    conditions = []

    for i in range(num_total):
        operator = "=" if i in eq_indices else random.choice(operators)

        if field == "city":
            value = random.choice(CITIES)
        elif field == "stationid":
            value = random.choice(STATION_IDS)
        elif field == "direction":
            value = random.choice(DIRECTIONS)
        elif field == "date":
            value = random.choice(DATES)
        elif field == "temp":
            value = random.randint(*TEMP_RANGE)
        elif field == "rain":
            value = round(random.uniform(*RAIN_RANGE), 2)
        elif field == "wind":
            value = random.randint(*WIND_RANGE)
        else:
            value = None

        conditions.append((field, operator, value))

    return conditions

def generate_subscriptions(num_subscriptions, sub_field_freqs, sub_op_freq):
    subscriptions = [{} for _ in range(num_subscriptions)]
    field_counts = {field: max(1, round(freq * num_subscriptions)) for field, freq in sub_field_freqs.items()}
    #exemplu: field_counts = { "temp": 8, "city": 5, etc }

    for field, count in field_counts.items():
        indices = random.sample(range(num_subscriptions), count)
        eq_prob = sub_op_freq.get(field, {"=": 0}).get("=", 0)
        if(eq_prob == 0):
            eq_prob = random.uniform(0, 0.3)
        eq_count = max(1, round(eq_prob * count))
        conditions = generate_condition(field, count, eq_count)

        for idx, cond in zip(indices, conditions):
            subscriptions[idx][field] = cond

    return [list(sub.values()) for sub in subscriptions]

def wrapper_generate_subscription(num_subscriptions, sub_field_freqs, sub_op_freq, _):
    return generate_subscriptions(num_subscriptions, sub_field_freqs, sub_op_freq)

def wrapper_generate_publication(_):
    return generate_publication()

def generate_data_parallel(num_processes, num_publications, num_subscriptions, sub_field_freqs, sub_op_freq):
    start_time = time.time()

    with Pool(num_processes) as p:
        publications = p.map(wrapper_generate_publication, range(num_publications))
        subscription_func = partial(wrapper_generate_subscription, num_subscriptions, sub_field_freqs, sub_op_freq)
        subscriptions = p.map(subscription_func, range(1))[0]

    duration = time.time() - start_time
    return publications, subscriptions, duration

def calculate_statistics(subscriptions):
    total_subs = len(subscriptions)
    field_counts = {field: 0 for field in ["stationid", "city", "temp", "rain", "wind", "direction", "date"]}
    eq_counts = {field: 0 for field in field_counts}

    for sub in subscriptions:
        fields_in_sub = {entry[0]: entry[1] for entry in sub}
        for field in field_counts:
            if field in fields_in_sub:
                field_counts[field] += 1
                if fields_in_sub[field] == "=":
                    eq_counts[field] += 1

    stats = []
    for field in field_counts:
        field_pct = (field_counts[field] / total_subs) * 100 if total_subs > 0 else 0
        eq_pct = (eq_counts[field] / field_counts[field]) * 100 if field_counts[field] > 0 else 0
        stats.append(f"{field}: {field_pct:.2f}% din subscriptii, '=' in {eq_pct:.2f}% din acestea")

    return "\n".join(stats)

if __name__ == "__main__":
    NUM_MESSAGES = int(input("Introduceti numarul total de mesaje: "))
    PUBLICATION_RATIO = float(input("Procentaj publicatii (ex. 0.5 pentru 50%): "))
    NUM_PUBLICATIONS = int(NUM_MESSAGES * PUBLICATION_RATIO)
    NUM_SUBSCRIPTIONS = NUM_MESSAGES - NUM_PUBLICATIONS

    FIELDS = ["stationid", "city", "temp", "rain", "wind", "direction", "date"]
    SUBSCRIPTION_FIELD_FREQUENCIES = {}
    SUBSCRIPTION_OPERATOR_FREQUENCY = {}

    for field in FIELDS:
        freq = float(input(f"Introduceti frecventa pentru {field} (0-1, 0 daca nu este folosit): "))
        if freq > 0:
            SUBSCRIPTION_FIELD_FREQUENCIES[field] = freq
            eq_freq = float(input(f"Introduceti frecventa pentru operatorul '=' la {field} (0-1, 0 daca nu conteaza): "))
            if eq_freq > 0:
                SUBSCRIPTION_OPERATOR_FREQUENCY[field] = {"=": eq_freq}

    PARALLELISM_LEVEL = int(input("Introduceti numarul de procese: "))

    pubs, subs, exec_time = generate_data_parallel(
        PARALLELISM_LEVEL, NUM_PUBLICATIONS, NUM_SUBSCRIPTIONS,
        SUBSCRIPTION_FIELD_FREQUENCIES, SUBSCRIPTION_OPERATOR_FREQUENCY
    )

    with open("publications.txt", "w") as f:
        for pub in pubs:
            f.write(json.dumps(pub) + "\n")

    with open("subscriptions.txt", "w") as f:
        for sub in subs:
            f.write(json.dumps(sub) + "\n---\n")
        f.write("\n" + calculate_statistics(subs))

    with open("results.txt", "w") as f:
        f.write(json.dumps({"threads": PARALLELISM_LEVEL, "execution_time": exec_time}, indent=2))

    print("Generare finalizata. Rezultatele au fost salvate in fisiere.")
