import json
import logging
import os

import requests
from flask import Flask, jsonify, request

from mqtt_brokers import criar_cliente, conectar_com_broker

API_URL = os.getenv(
    "API_URL",
    "http://api:5000"
)

INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN")
MQTT_TOPIC_STATUS = os.getenv("MQTT_TOPIC_STATUS", "cm/ar/+/status")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

mqtt_client = None
mqtt_broker = None

def extrair_atuador_do_topico(topico: str):

    partes = topico.split("/")

    if len(partes) < 4:
        return None

    return partes[2]


# ============================================================
# Callbacks MQTT
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties=None
):

    if reason_code == 0:

        logger.info(
            "Conectado ao broker MQTT"
        )

        client.subscribe(
            MQTT_TOPIC_STATUS
        )

        logger.info(
            "Inscrito no tópico: %s",
            MQTT_TOPIC_STATUS
        )

    else:

        logger.error(
            "Erro ao conectar ao MQTT: %s",
            reason_code
        )

def on_disconnect(
    client,
    userdata,
    disconnect_flags=None,
    reason_code=None,
    properties=None
):

    logger.warning(
        "Cliente MQTT desconectado. reason_code=%s",
        reason_code
    )

def on_message(
    client,
    userdata,
    msg
):

    try:

        payload = json.loads(
            msg.payload.decode("utf-8")
        )

        if not isinstance(payload, dict):

            raise ValueError(
                "O payload MQTT não é um objeto JSON"
            )


        # Acrescenta informações que vieram do MQTT

        payload["atuador"] = (
            extrair_atuador_do_topico(
                msg.topic
            )
        )

        payload["topico"] = msg.topic


        logger.debug(
            "Mensagem MQTT recebida: tópico=%s payload=%s",
            msg.topic,
            payload
        )


        # ----------------------------------------------------
        # Envia status para a API
        # ----------------------------------------------------

        resposta = requests.post(

            f"{API_URL}/internal/mqtt/status",

            json=payload,

            headers={
                "X-Internal-Token":
                    INTERNAL_API_TOKEN or ""
            },

            timeout=10
        )

        resposta.raise_for_status()


        logger.info(
            "Status MQTT enviado para API"
        )


    except json.JSONDecodeError:

        logger.exception(
            "Payload MQTT inválido: %r",
            msg.payload
        )


    except requests.RequestException:

        logger.exception(
            "Não foi possível enviar "
            "o status MQTT para a API"
        )


    except Exception:

        logger.exception(
            "Erro ao processar mensagem MQTT"
        )


# ============================================================
# Inicialização MQTT
# ============================================================

def iniciar_mqtt():

  global mqtt_client
  global mqtt_broker


  mqtt_client = criar_cliente(
    client_id="superar-mqtt-service"
  )


  mqtt_client.on_connect = on_connect
  mqtt_client.on_message = on_message
  mqtt_client.on_disconnect = on_disconnect


  # conecta_com_broker() já implementa seu
  # broker primário + fallback

  mqtt_broker = conectar_com_broker(
    mqtt_client
  )


  logger.info(
    "Broker MQTT selecionado: %s (%s:%s)",
    mqtt_broker.nome,
    mqtt_broker.host,
    mqtt_broker.port
  )


  # MUITO IMPORTANTE:
  #
  # loop_start() roda MQTT em outra thread.
  #
  # Não usamos loop_forever(), porque Flask
  # precisa utilizar a thread principal.

  mqtt_client.loop_start()


# ============================================================
# Callbacks FLASK
# ============================================================

@app.get("/health")
def health():

  conectado = (
    mqtt_client is not None
    and mqtt_client.is_connected()
  )

  return jsonify({
    "status": "ok",
    "mqtt_connected": conectado,
    "broker": (
      mqtt_broker.nome
      if mqtt_broker
      else None
    )
  })

@app.post("/publish")
def publicar():
  dados = request.get_json(
    silent=True
  )

  if not isinstance(dados, dict):
    return jsonify({
      "status": "erro",
      "mensagem":
        "O corpo da requisição deve ser JSON"
    }), 400


  topic = dados.get("topic")
  payload = dados.get("payload")

  qos = dados.get(
    "qos",
    0
  )

  retain = dados.get(
    "retain",
    False
  )


  if not topic:
    return jsonify({
      "status": "erro",
      "mensagem":
        "O campo 'topic' é obrigatório"
    }), 400


  if payload is None:
    return jsonify({
      "status": "erro",
      "mensagem":
        "O campo 'payload' é obrigatório"
    }), 400

  if mqtt_client is None:
    return jsonify({
      "status": "erro",
      "mensagem":
        "Cliente MQTT não inicializado"
    }), 503


  if not mqtt_client.is_connected():
    return jsonify({
      "status": "erro",
      "mensagem":
        "MQTT não está conectado"
    }), 503

  try:
    mensagem = json.dumps(
      payload
    )


    resultado = mqtt_client.publish(
      topic,
      mensagem,
      qos=qos,
      retain=retain
    )


    # Aguarda confirmação do envio para o cliente Paho.
    # Não significa que o ESP recebeu,
    # apenas que a publicação foi processada
    # pelo cliente MQTT.

    resultado.wait_for_publish(
      timeout=5
    )

    if resultado.rc != 0:
      return jsonify({
        "status": "erro",
        "mensagem":
          "Falha ao publicar no broker MQTT",
        "codigo": resultado.rc
      }), 503


    logger.info(
      "MQTT publicado: %s",
      topic
    )


    return jsonify({
      "status": "ok",
      "topic": topic
    })


  except Exception as erro:
    logger.exception(
      "Erro ao publicar mensagem MQTT"
    )

    return jsonify({
      "status": "erro",
      "mensagem": str(erro)
    }), 500

@app.post("/ar/comando")
def publicar_comando_ar():

  dados = request.get_json(
    silent=True
  )

  if not isinstance(dados, dict):

    return jsonify({
      "status": "erro",
      "mensagem": "JSON inválido"
    }), 400

  atuador = dados.get(
    "atuador"
  )

  payload = dados.get(
    "payload"
  )

  if not atuador:
    return jsonify({
      "status": "erro",
      "mensagem":
        "O campo 'atuador' é obrigatório"
    }), 400


  if payload is None:

    return jsonify({
      "status": "erro",
      "mensagem":
        "O campo 'payload' é obrigatório"
    }), 400


  if mqtt_client is None:

    return jsonify({
      "status": "erro",
      "mensagem":
        "Cliente MQTT não inicializado"
    }), 503


  if not mqtt_client.is_connected():

    return jsonify({
      "status": "erro",
      "mensagem":
        "Broker MQTT não conectado"
    }), 503


  try:
    # O conhecimento sobre a estrutura
    # dos tópicos fica SOMENTE neste serviço.

    topico = (
      f"cm/ar/{atuador}/cmd"
    )


    resultado = mqtt_client.publish(
      topico,
      json.dumps(payload)
    )


    resultado.wait_for_publish(
      timeout=5
    )


    if resultado.rc != 0:
      return jsonify({
        "status": "erro",
        "mensagem":
          "Falha na publicação MQTT",
        "codigo": resultado.rc
      }), 503


    logger.info(
      "Comando publicado para %s",
      atuador
    )


    return jsonify({
      "status": "ok",
      "atuador": atuador,
      "topic": topico
    })

  except Exception as erro:
    logger.exception(
      "Erro ao publicar comando MQTT"
    )

    return jsonify({
      "status": "erro",
      "mensagem": str(erro)
    }), 500


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
  iniciar_mqtt()

  app.run(
    host="0.0.0.0",
    port=5002,
    debug=False,
    threaded=True
  )