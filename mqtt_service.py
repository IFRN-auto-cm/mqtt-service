import json
import logging
import os

import paho.mqtt.client as mqtt
import requests
from mqtt_brokers import criar_cliente, conectar_com_broker


API_URL = os.getenv(
    "API_URL",
    "http://api:5000"
)

INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN")
MQTT_TOPIC_STATUS = os.getenv("MQTT_TOPIC_STATUS")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def extrair_atuador_do_topico(topico):
    # Exemplo:
    # cm/ar/irClient-D8BC38A94716/status

    partes = topico.split("/")

    if len(partes) < 4:
        return None

    return partes[2]


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.debug("Conectado ao broker MQTT")
        client.subscribe(MQTT_TOPIC_STATUS)
    else:
        logger.error(
            "Erro ao conectar ao MQTT: %s",
            reason_code
        )


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload)

        if not isinstance(payload, dict):
            raise ValueError("O payload não é um objeto JSON")

        payload["atuador"] = extrair_atuador_do_topico(msg.topic)
        payload["topico"] = msg.topic

        resposta = requests.post(
            f"{API_URL}/internal/mqtt/status",
            json=payload,
            headers={
                "X-Internal-Token": INTERNAL_API_TOKEN
            },
            timeout=10
        )

        resposta.raise_for_status()

        logger.info(
            "Mensagem registrada pela API: %s",
            resposta.json()
        )

    except json.JSONDecodeError:
        logger.exception(
            "Payload MQTT inválido: %r",
            msg.payload
        )

    except requests.RequestException:
        logger.exception(
            "Não foi possível enviar os dados para a API"
        )

    except Exception:
        logger.exception(
            "Erro ao processar a mensagem MQTT"
        )


def main():
    client = criar_cliente( client_id="superar-status-consumer" )
    client.on_connect = on_connect
    client.on_message = on_message

    broker = conectar_com_broker(client)

    logger.debug(
        f"Conectado ao broker {broker.nome}: "
        f"{broker.host}:{broker.port}"
    )

    client.loop_forever()

if __name__ == "__main__":
    main()
