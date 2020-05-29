from ranch.enums import Acks, Order
from ranch.characteristics import Characteristics as Cs
from ranch.interpolation import Interpolation, FuncNotFoundError
from ranch.processor import Processor
import os
import sys
import numpy as np
import streamlit as st
import flask
import scipy.interpolate

from concurrent.futures import TimeoutError
from confluent_kafka import KafkaError, KafkaException, Consumer
from confluent_kafka.admin import AdminClient


class ValidationError(Exception):
    pass


def validate_request(json):
    if json == None:
        raise ValidationError("invalid json in request")
    if not 'brokers' in json or not isinstance(json['brokers'], list):
        raise ValidationError("invalid brokers in request")
    if not 'semantic' in json or not isinstance(json['semantic'], str) or not json['semantic'] in ['at-least-once', 'at-most-once', 'exactly-once']:
        raise ValidationError("invalid semantic in request. possible values are at-least-once, at-most-once or exactly-once")
    if not 'topic' in json or not isinstance(json['topic'], str):
        raise ValidationError("invalid topic name in request")
    if not 'messageSize' in json or not isinstance(json['messageSize'], int):
        raise ValidationError("invalid messageSize in request. must be valid integer value")
    if not 'strictOrder' in json or not isinstance(json['strictOrder'], bool):
        raise ValidationError("invalid strictOrder in request")
    return

def resolve_acks(semantic):
    acks = Acks.ONE
    idempotence = False
    if semantic == 'at-least-once':
        acks = Acks.ONE
    elif semantic == 'at-most-once':
        acks = Acks.NO
    else:
        acks = Acks.ALL
        idempotence = True

    return acks, idempotence


def resolve_inflight(strict_order):
    inflight = Order.ANY
    if strict_order:
        inflight = Order.STRICT
    else:
        inflight = Order.ANY

    return inflight

@st.cache(hash_funcs={scipy.interpolate.interpolate.interp1d: lambda _: None})
def load_funcs():
    basepath = os.path.abspath(".")
    # try:
    #     basepath = sys._MEIPASS
    # except Exception:
    #     basepath = os.path.abspath(".")
    batch_file = 'funcs/batches.npy'
    linger_file = 'funcs/lingers.npy'
    batch_file = os.path.join(basepath, batch_file)
    linger_file = os.path.join(basepath, linger_file)
    batch_funcs = np.load(batch_file, allow_pickle=True)
    linger_funcs = np.load(linger_file, allow_pickle=True)
    batch_interpolation = Interpolation(batch_funcs.item())
    linger_interpolation = Interpolation(linger_funcs.item())
    return batch_interpolation, linger_interpolation


def print_config(brokers, acks, idempotence, inflight, compression_type, max_message, batch_bytes, linger_ms, as_dict=True):
    if as_dict:
        return {
            "bootstrap.servers": ','.join([str(broker)+":9092" for broker in brokers]),
            "acks": acks.value,
            "enable.idempotence": idempotence,
            "max.in.flight.requests.per.connection": inflight.value,
            "compression.type": compression_type,
            "buffer.memory": max_message,
            "batch.size": batch_bytes,
            "linger.ms": linger_ms
        }
    return f"""
    bootstrap.servers={','.join([str(broker)+":9092" for broker in brokers])}\n
    acks={acks.value}\n
    enable.idempotence={str(idempotence).lower()}\n
    max.in.flight.requests.per.connection={inflight.value}\n
    compression.type={compression_type}\n
    buffer.memory={max_message}\n
    batch.size={batch_bytes}\n
    linger.ms={linger_ms}\n
    """

@st.cache(hash_funcs={scipy.interpolate.interpolate.interp1d: lambda _: None})
def process(req, batch_interpolation, linger_interpolation, as_dict=True):
    kafka_admin = AdminClient({'bootstrap.servers': ','.join([str(broker)+":9092" for broker in req['brokers']])})
    kafka_consumer = AdminClient({'bootstrap.servers': ','.join([str(broker)+":9092" for broker in req['brokers']])})
    acks, idempotence = resolve_acks(req['semantic'])
    inflight = resolve_inflight(req['strictOrder'])

    try:
        c = Cs(kafka_admin, kafka_consumer, req['brokers'], req['topic'])
        processor = Processor(c, batch_interpolation, linger_interpolation, req['messageSize'], acks, inflight)

        compression = processor.get_compression()
        max_message = processor.get_max_buffer()
        batch = processor.get_batch_size()
        linger = processor.get_linger_ms()
    except KafkaException as ke:
        error = ke.args[0]
        if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            return {"error": f"topic with name {req['topic']} doesn't exist"}, 400
        else:
            return {"error": error.str()}, 500
    except TimeoutError:
        return {"error": "timeout while connecting to broker"}, 400
    except FuncNotFoundError:
        return {"error": "wrong value for acks or inflight"}, 400


    return print_config(
        req['brokers'],
        acks,
        idempotence,
        inflight,
        compression,
        max_message,
        batch,
        linger,
        as_dict=as_dict
    ), 200

def main():
    if not hasattr(st, 'already_started_server'):
        st.already_started_server = True

        st.write('''
            The first time this script executes it will run forever because it's
            running a Flask server.

            Just close this browser tab and open a new one to see your Streamlit
            app.
        ''')

        app = flask.Flask(__name__)

        @app.route('/', methods=['POST'])
        def serve_foo():
            """
            {
                "brokers": [
                    "host1",
                    "host2",
                ],
                "topic": "test",
                "semantic": "at-least-once",
                "messageSize": 100,
                "strictOrder": true
            }
            """
            req = flask.request.json

            try:
                validate_request(req)
            except ValidationError as e:
                return {"error": str(e)}, 400

            batch_interpolation, linger_interpolation = load_funcs()

            return process(req, batch_interpolation, linger_interpolation, as_dict=True)

        app.run(port=8888)

    batch_interpolation, linger_interpolation = load_funcs()
    brokers_string = st.sidebar.text_input('Адреса брокеров', 'testing-kafka-dev01,testing-kafka-dev02,testing-kafka-dev03')
    topic = st.sidebar.text_input('Название топика', 'test-snappy')
    semantic = st.sidebar.selectbox('Семантика доставки', ['at-least-once', 'at-most-once', 'exactly-once'], format_func=lambda x: x.replace('-', ' ').capitalize())
    message_size = st.sidebar.number_input('Размер сообщения', min_value=128, max_value=4194304)
    strict_order = st.sidebar.checkbox('Строгий порядок сообщений', False)
    req = {
        'brokers': brokers_string.split(','),
        'topic': topic,
        'semantic': semantic,
        'messageSize': message_size,
        'strictOrder': strict_order,
    }
    config, _ = process(req, batch_interpolation, linger_interpolation, as_dict=False)
    st.write(config)

if __name__ == '__main__':
    main()