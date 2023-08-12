from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from config import Config
from datetime import datetime
from flask import request, jsonify
import asyncio
import aiohttp
from sqlalchemy import create_engine
import logging
import threading

logging.basicConfig(filename='app.log', level=logging.INFO)


app = Flask(__name__)
app.config.from_object(Config)

engine = create_engine(
    app.config['SQLALCHEMY_DATABASE_URI'],
    pool_size=10, max_overflow=20
)

db = SQLAlchemy(app)
migrate = Migrate(app, db)


class CounterData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    counter_id = db.Column(db.Integer, nullable=False)
    current_current = db.Column(db.Float, nullable=False)
    total_energy = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (f'Counter({self.id}, current_current={self.current_current},'
                f' total_energy={self.total_energy}, timestamp={self.timestamp})')


@app.route('/add_counter', methods=['POST'])
def add_counter():
    try:
        data = request.json
        counter_id = data['id']
        existing_counter = CounterData.query.filter_by(counter_id=counter_id).first()

        if existing_counter:
            return jsonify({'error': f'Счетчик с id - {counter_id} уже существует'}), 400

        counter = CounterData(counter_id=counter_id, current_current=data['A'], total_energy=data['kW'])
        db.session.add(counter)
        db.session.commit()
        return jsonify({'message': f'Счетчик с id {counter_id} успешно добавлен!'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/get_current_state', methods=['GET'])
def get_current_state():
    try:
        counter_id = request.args.get('id')

        if not counter_id:
            return jsonify({'error': 'Отсутствуют id счетчика'}), 400

        existing_counter = CounterData.query.filter_by(counter_id=counter_id).first()
        if not existing_counter:
            return jsonify({'error': f'Счетчик - {counter_id} еще не зарегистрирован в БД'}), 404

        latest_data = CounterData.query.filter_by(counter_id=counter_id).order_by(CounterData.timestamp.desc()).first()
        if not latest_data:
            return jsonify({'error': f'Данные для счетчика - {counter_id} отсутствуют в БД'}), 404

        response = {
            'counter_id': latest_data.counter_id,
            'current_current': latest_data.current_current,
            'total_energy': latest_data.total_energy,
            'timestamp': latest_data.timestamp
        }

        return jsonify(response), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/get_statistics/<int:counter_id>', methods=['GET'])
def get_statistics(counter_id):
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        if not start_date or not end_date:
            return jsonify({'error': 'Не указано начало периода start_date или конец end_date в запросе'}), 400

        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

        statistics = CounterData.query.filter_by(
            counter_id=counter_id
        ).filter(
            CounterData.timestamp >= start_date,
            CounterData.timestamp <= end_date
        ).order_by(
            CounterData.timestamp
        ).all()

        if not statistics:
            return jsonify({'message': 'Нет данных на запрашиваемый период'}), 404

        statistics_data = [{'current_current': entry.current_current, 'total_energy': entry.total_energy,
                            'timestamp': entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')} for entry in statistics]
        return jsonify(statistics_data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/delete_counter/<int:counter_id>', methods=['DELETE'])
def delete_counter(counter_id):
    try:
        counter = CounterData.query.filter_by(counter_id=counter_id).first()
        if not counter:
            return jsonify({'message': f'Счетчик {counter_id} не найден'}), 404
        # Удаляем все данные связанные с указанным счетчиком
        data_to_delete = CounterData.query.filter_by(counter_id=counter_id)
        data_to_delete.delete()
        db.session.commit()
        return jsonify({'message': f'Все данные счетчика {counter_id} успешно удалены из БД'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400


# Функция для асинхронного получения данных счетчика
async def fetch_data(counter_id, port_number):
    try:
        async with aiohttp.ClientSession() as session:
            url = f'http://127.0.0.1:{port_number}/get_current_state'
            async with session.get(url) as response:
                data = await response.json()

                if all(key in data for key in ['A', 'id', 'kW']):
                    return counter_id, data
                else:
                    logging.error(f'Ошибка в ответе счетчика {counter_id}: {data}')
                    return None

    except Exception as e:
        logging.error(f'Ошибка при запросе счетчика {counter_id}: {e}')
        return None


# Функция для запуска асинхронных задач
async def start_background_tasks(app):
    loop = asyncio.get_event_loop()
    num_counters = 100  # Количество счетчиков
    polling_interval = 30  # Интервал опроса в секундах
    tasks = []

    for i in range(num_counters):
        counter_id = 1 + i
        port_number = 9001 + i
        task = loop.create_task(fetch_data(counter_id, port_number))
        tasks.append(task)

    while True:
        with app.app_context():
            results = await asyncio.gather(*tasks)

            for result in results:
                if result is not None:
                    counter_id, data = result
                    existing_counter = CounterData.query.filter_by(counter_id=counter_id).first()
                    if existing_counter:
                        logging.info(f'Счетчик {counter_id} уже зарегистрирован в БД. Добавляем новые данные.')
                        counter = CounterData(
                            counter_id=counter_id,
                            current_current=data['A'],
                            total_energy=data['kW']
                        )
                        db.session.add(counter)
                        db.session.commit()
                        logging.info(f'Данные счетчика {counter_id} успешно добавлены')
                    else:
                        logging.warning(f'Счетчик {counter_id} еще не зарегистрирован в БД. Данные не записаны.')

        await asyncio.sleep(polling_interval)

        # Перезапускаем задачи
        tasks = []
        for i in range(num_counters):
            counter_id = 1 + i
            port_number = 9001 + i
            task = loop.create_task(fetch_data(counter_id, port_number))
            tasks.append(task)


def run_background_tasks():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_background_tasks(app))


if __name__ == '__main__':
    background_thread = threading.Thread(target=run_background_tasks)
    background_thread.start()
    app.run(debug=True)

