from flask import Flask, jsonify, request
import random
import time
import threading

app_ports = list(range(9001, 9102))  # Список портов от 9001 до 9101


def generate_counter_data(port):
    return {'id': port - 9000, 'A': 50, 'kW': 680}


def simulate_counter_data(port):
    while True:
        time.sleep(5)  # Ждем 5 секунд
        current_A = counters[port]['A'] + random.randint(1, 5)
        current_kW = counters[port]['kW'] + random.randint(3, 10)
        counters[port]['A'] = current_A
        counters[port]['kW'] = current_kW


app = Flask(__name__)

counters = {port: generate_counter_data(port) for port in app_ports}

threads = []


@app.route('/get_current_state', methods=['GET'])
def get_current_state():
    port = int(request.environ.get('SERVER_PORT'))
    counter_data = counters.get(port, {})
    return jsonify(counter_data), 200


if __name__ == '__main__':
    for port in app_ports:
        thread = threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': port, 'debug': False})
        thread.daemon = True
        thread.start()
        threads.append(thread)
        simulate_thread = threading.Thread(target=simulate_counter_data, args=(port,))
        simulate_thread.daemon = True
        simulate_thread.start()

    for thread in threads:
        thread.join()
