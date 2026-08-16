import os
import time
from dataclasses import dataclass

import logging
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Broker:
    host: str
    port: int
    nome: str


def brokers_configurados() -> list[Broker]:
    return [
        Broker(
            host=os.getenv("MQTT_BROKER_PRIMARY_HOST", "10.57.0.10"),
            port=int(os.getenv("MQTT_BROKER_PRIMARY_PORT", "1883")),
            nome="primário",
        ),
        Broker(
            host=os.getenv("MQTT_BROKER_SECONDARY_HOST", "mqtt-broker"),
            port=int(os.getenv("MQTT_BROKER_SECONDARY_PORT", "1883")),
            nome="secundário",
        ),
    ]


def criar_cliente(client_id: str = "") -> mqtt.Client:
    try:
        cliente = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
    except (AttributeError, TypeError):
        cliente = mqtt.Client(client_id=client_id)

    usuario = os.getenv("MQTT_USERNAME") or os.getenv("USUARIO")
    senha = os.getenv("MQTT_PASSWORD") or os.getenv("SENHA")
    if usuario:
        cliente.username_pw_set(usuario, senha)

    return cliente


def conectar_com_broker(
    cliente: mqtt.Client,
    keepalive: int = 60,
    tentativas_por_broker: int = 1,
) -> Broker:
    ultimo_erro: Exception | None = None

    for broker in brokers_configurados():
        for tentativa in range(1, tentativas_por_broker + 1):
            try:
                cliente.connect(broker.host, broker.port, keepalive)
                return broker
            except OSError as erro:
                ultimo_erro = erro
                logger.debug(
                    f"Falha no broker {broker.nome} "
                    f"{broker.host}:{broker.port} "
                    f"(tentativa {tentativa}): {erro}"
                )
                time.sleep(1)

    raise ConnectionError("Nenhum broker MQTT está disponível") from ultimo_erro
