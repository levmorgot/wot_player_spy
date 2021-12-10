import requests
import time
import datetime
from threading import Thread

from common.db import SpyDB
from common.kafka import connect_kafka_producer, publish_message, connect_kafka_consumer
import config

db = SpyDB()


def _get_player_data(player_id):
    resp = requests.get(
        f"{config.core_url}account/info/?application_id={config.application_id}&account_id={player_id}").json()
    if resp["status"] == "ok":
        return resp["data"]


def update_info():
    with connect_kafka_consumer("get_players") as consumer:
        players = []
        for msg in consumer:
            service_name = msg.key.decode("utf-8")
            print(service_name)
            for playerInfo in db.get_all_players():
                player_id, username, last_battle_time = playerInfo
                data = _get_player_data(player_id)
                time.sleep(0.2)
                new_last_battle_time = data[f"{player_id}"]["last_battle_time"]
                if last_battle_time != new_last_battle_time:
                    db.update_last_battle(player_id, new_last_battle_time)
                    player_data = data[f"{player_id}"]
                    players.append(player_data)
                    print(datetime.datetime.fromtimestamp(new_last_battle_time))
                    with connect_kafka_producer() as producer:
                        publish_message(producer, f"{service_name}", f"{service_name}", f"{players}")


def search_player():
    with connect_kafka_consumer("search_player") as consumer:
        for msg in consumer:
            chat_id = int(msg.key)
            username = msg.value.decode("utf-8")
            resp = requests.get(
                f"{config.core_url}account/list/?application_id={config.application_id}&search={username}").json()
            if resp["status"] == "ok":
                data = resp["data"]
                with connect_kafka_producer() as producer:
                    for player_info in data:
                        publish_message(
                            producer,
                            'fond_players',
                            f"{chat_id}",
                            f"{player_info}"
                        )


def chose_player():
    with connect_kafka_consumer("chosen_player") as consumer:
        for msg in consumer:
            chat_id = int(msg.key)
            account_id = int(msg.value)
            if account_id:
                data = _get_player_data(account_id)
                player_data = data[f"{account_id}"]
                with connect_kafka_producer() as producer:
                    publish_message(
                        producer,
                        'player_info',
                        f"{chat_id}",
                        f"{player_data}"
                    )


def add_player():
    with connect_kafka_consumer("add_player") as consumer:
        for msg in consumer:
            account_id = int(msg.value)
            if account_id:
                data = _get_player_data(account_id)
                player_data = data[f"{account_id}"]
                db.new_player(
                    player_data["account_id"],
                    player_data["nickname"],
                    player_data["last_battle_time"]
                )


if __name__ == '__main__':
    update_th = Thread(target=update_info)
    update_th.start()

    search_player_th = Thread(target=search_player)
    search_player_th.start()

    chose_player_th = Thread(target=chose_player)
    chose_player_th.start()

    add_player_th = Thread(target=add_player)
    add_player_th.start()
