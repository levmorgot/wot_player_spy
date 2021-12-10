from datetime import datetime
from mysql.connector import connect

import config


class SpyDB:

    def __init__(self):
        self.db = connect(
                host="localhost",
                user=config.db_username,
                password=config.db_pass,
                auth_plugin='mysql_native_password',
                database=config.db_name,
        )
        self.sql = self.db.cursor()
        self._create_db()

    def _create_db(self):
        self.sql.execute("""CREATE TABLE IF NOT EXISTS players(
            id INT AUTO_INCREMENT PRIMARY KEY,
            player_id INT,
            username VARCHAR(100),
            last_battle_time INT
)""")
        self.db.commit()

    def new_player(self, player_id, username, last_battle_time):
        self.sql.execute(f"SELECT player_id FROM players WHERE player_id = '{player_id}'")
        if self.sql.fetchone() is None:
            self.sql.execute(f"""
                INSERT INTO players (player_id, username, last_battle_time)
                VALUES
                    ({player_id}, '{username}', {last_battle_time})
    """)
            self.db.commit()
            print("Добавлен новый игрок {player_id} / {time}".format(
                player_id=player_id,
                time=datetime.now()))

    def update_last_battle(self, player_id, last_battle_time):
        self.sql.execute(f"UPDATE players SET last_battle_time = '{last_battle_time}' WHERE player_id = '{player_id}'")
        print("Вернулся пользователь {player_id}/{last_battle_time} {time}".format(
            player_id=player_id,
            last_battle_time=last_battle_time,
            time=datetime.now()))
        self.db.commit()

    def get_all_players(self):
        self.sql.execute(f"SELECT player_id, username, last_battle_time FROM players")
        result = self.sql.fetchall()
        return [player for player in result]

    def find_player_by_username(self, username):
        self.sql.execute(f"SELECT player_id, username, last_battle_time FROM players WHERE username = '{username}'")
        result = self.sql.fetchone()
        return result

    def find_player_by_player_id(self, player_id):
        self.sql.execute(f"SELECT player_id, username, last_battle_time FROM players WHERE player_id = '{player_id}'")
        result = self.sql.fetchone()
        return result
