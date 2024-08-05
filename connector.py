import mysql.connector


class ConnectorRead:
    def __init__(self, config):
        print(" Opening connection...")
        self.connection = mysql.connector.connect(**config)
        self.cursor = self.connection.cursor()
        print("Connection established")

    def __del__(self):
        print("Closing connection...")
        self.cursor.close()
        self.connection.close()
        print("Connection closed")

    @staticmethod
    def get_connect(database="sakila"):
        dbconfig = {
            'host': 'ich-db.ccegls0svc9m.eu-central-1.rds.amazonaws.com',
            'user': 'ich1',
            'password': 'password',
            'database': 'sakila'
        }

        return mysql.connector.connect(**dbconfig)
        # return connection.cursor()


class ConnectorWrite:
    def __init__(self, config):
        print(" Opening connection...")
        self.connection = mysql.connector.connect(**config)
        self.cursor = self.connection.cursor()
        print("Connection established")

    def __del__(self):
        print("Closing connection...")
        self.cursor.close()
        self.connection.close()
        print("Connection closed")

    @staticmethod
    def get_connect(database="sakila"):
        dbconfig = {
            'host': 'mysql.itcareerhub.de',
            'user': 'ich1',
            'password': 'ich1_password_ilovedbs',
            'database': '310524ptm_O_Shevchenko'
        }

        return mysql.connector.connect(**dbconfig)
