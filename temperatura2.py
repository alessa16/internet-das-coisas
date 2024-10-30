import requests_cache
import pandas as pd
from retrying import retry # Biblioteca corrigida
import json
import time
import paho.mqtt.client as mqtt

# Setup de cache para evitar requisições repetidas em curto período
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)

# Função com tentativa de retry caso a requisição falhe
@retry(stop_max_attempt_number=5, wait_fixed=200) # faz parte da biblioteca `retrying` e serve para adicionar uma lógica de reprodução automática a uma função, caso ela falhe ou lance uma exceção.
def get_weather_data(url, params):
    response = cache_session.get(url, params=params)
    response.raise_for_status()  # Levanta exceção para erros HTTP
    return response.json()

# Função que será chamada ao conectar no broker MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("weather/data")

# Função para publicar os dados do clima via MQTT
def publish_weather_data(client, weather_data):
    payload = json.dumps(weather_data)
    client.publish("weather/data", payload)

def main():
    MQTT_BROKER = "192.168.56.101"  # Endereço do broker MQTT
    MQTT_TOPIC = "test_channel"
    username = "mosquitto"
    password = "dietpi"

    # Definição do endpoint da API Open-Meteo e parâmetros
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -5.6344,  # Coordenadas de Ceará Mirim
        "longitude": -35.4256,
        "hourly": ["temperature_2m", "rain"],
        "timezone": "America/Fortaleza"  # Zona de tempo correta
    }

    # Obtém os dados da API
    weather_data = get_weather_data(url, params=params)

    # Processamento dos dados horários
    hourly_data = weather_data['hourly']
    hourly_time = pd.to_datetime(hourly_data['time'])  # Converte o tempo em formato datetime
    hourly_temperature_2m = hourly_data['temperature_2m']
    hourly_rain = hourly_data['rain']

    # Cria um dicionário com os dados processados
    weather_info = {
        "time": hourly_time.strftime('%Y-%m-%d %H:%M:%S').tolist(),  # Formata a data e hora
        "temperature_2m": hourly_temperature_2m,
        "rain": hourly_rain
    }

    # Inicializa o cliente MQTT e conecta ao broker
    client = mqtt.Client()
    client.on_connect = on_connect
    client.username_pw_set(username, password)  # Define o usuário e senha, se necessário
    client.connect(MQTT_BROKER, 1883, 60)

    # Publica os dados do clima
    publish_weather_data(client, weather_info)

    # Loop infinito para manter a conexão ativa e escutar mensagens (opcional)
    client.loop_forever()

if __name__ == "__main__":
    main()
