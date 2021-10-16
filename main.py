import os
import socket
import datetime
import sys
import tqdm

MAX_QUERY_SIZE = 1

SOCKET_PORT = 50015
SOCKET_HOST = '127.0.0.1'
CONNECTION_DATA = (SOCKET_HOST, SOCKET_PORT)

BUFFER_SIZE = 1024 * 32 #8KB
SEPARATOR = "<SEPARATOR>"

# битрейт
def speed(buf, t0, t1):
    return round(len(buf)/(t1-t0)/1024**2, 2)


class TCPServer:
    SERVER_STOPPED_MESSAGE = b'SERVER STOPPED!' # b-префикс означает bytes строковый литерал
    LOG_FILE = 'server_log_{}.log'

    RECEIVE_BUFFER_SIZE = 1024
    TIMEOUT = 60

    LOG_DIR = 'logs'
    STORAGE_DIR = 'storage'
    LAST_IP = '127.0.0.1'
    PREV_COMMAND = '-'
    PREV_FILE = '-'

    def __init__(self, host='', port=SOCKET_PORT, max_client_count=MAX_QUERY_SIZE, sock=None, log_file=None): # конструктор класса
        self.max_client_count = max_client_count
        self.host = host
        self.port = port
        self.server_address = (self.host, self.port)
        self.socket = sock

        self.log_file = log_file
        self.startLogging()

        self.connections = []

    def startLogging(self):
        cur_dir = os.path.abspath(os.path.curdir) # Получить абсолютный путь файла или каталога
        storage_path = os.path.join(cur_dir, self.STORAGE_DIR) # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *STORAGE_DIR
        log_path = os.path.join(cur_dir, self.LOG_DIR) # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *LOG_DIR


        if not os.path.exists(storage_path):
            os.mkdir(storage_path) # создает каталог с именем storage_path

        if not os.path.exists(log_path):
            os.mkdir(log_path) # создает каталог с именем log_path

        log_file = os.path.join(
            log_path,
            self.LOG_FILE.format(datetime.datetime.now().strftime('%d.%m.%Y__%H.%M.%S'))
        )

        if not self.log_file or self.log_file.closed:
            self.log_file = open(log_file, 'w')

        self.log('server created')
        self.log('server storage path {}'.format(storage_path))
        self.log('server log path {}'.format(log_path))

        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)

        self.log('server ip address: port = {}:{}'.format(ip_address, self.port))
        self.log_file.close()
        self.LOG_FILE = log_file

    def socketOpen(self):
        self.socket.listen(self.max_client_count) # подготавливает сокет для приема соединений, означает максимальное количество подключений, которые операционная система может поставить в очередь для этого сокета
        self.log('open socket for {} clients'.format(self.max_client_count))

    def createSocket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # создать TCP-сокет семейства AF_INET типа потоковый сокет
        #  устанавливает значение опции сокета
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60) # Время (в секундах) простоя (idle) соединения, по прошествии которого TCP начнёт отправлять проверочные пакеты (keepalive probes), если для сокета включён параметр SO_KEEPALIVE
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60) # Время в секундах между отправками отдельных проверочных пакетов (keepalive probes).
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10) # Максимальное число проверок (keepalive probes) TCP, отправляемых перед сбросом соединения.

        sock.bind(self.server_address) # bind () используется, когда сокет необходимо сделать сокетом сервера

        self.log('create socket {}'.format(sock))

        return sock

    def clientWait(self):
        conn, addr = self.socket.accept() # Метод Socket.accept() принимает соединение. Сокет должен быть привязан к адресу и прослушивать соединения
        self.connections.append((conn, addr))
        self.log('new client connected {}'.format(addr))
        return conn, addr

    def clientProcessing(self, connection, addr):
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)

        while True:
            data = connection.recv(self.RECEIVE_BUFFER_SIZE)
            # print(data)
            if not data:
                return

            command, *params = data.split(b' ') #  разбивает строку на части
            self.log('client {} send commend {} with params {}'.format(addr, command, params))

            if command == b'ping':
                connection.send(b'pong')
            elif command == b'pong':
                connection.send(b'ping')
            elif command == b'cont':
                # print('dddd')
                # print(addr)
                # print(self.LAST_IP)
                if addr[0] == self.LAST_IP:
                    # print('c')
                    if self.PREV_COMMAND == 'U':
                        # print('a u')
                        # print('a', self.PREV_COMMAND)
                        self.upload_file(connection, self.PREV_FILE,params[0].decode(encoding='utf-8'))
                    elif self.PREV_COMMAND == 'D':
                        # print('a d')
                        # print('a', self.PREV_COMMAND)
                        self.download_file(connection, self.PREV_FILE,params[0].decode(encoding='utf-8'))

                self.LAST_IP = '-'
            elif command == b'help':
                connection.send(b'''help - to see list of commands
                ping - test that the server is alive
                kill - to stop server
                echo - to resent message to a client
                upload - to upload file to the server `upload file_name_on_your_machine.extension`
                download - to download file from a server `download file_name_on_server`
                time - get server time
                ''')
            elif command == b'kill':
                connection.send(b'GoodBy LOX!')
                return -1
            elif command == b'echo':
                connection.send(b' '.join(params))
            elif command == b'upload':
                self.upload_file(connection, params[0].decode(encoding='utf-8'))

            elif command == b'download':
                # connection.settimeout(20)
                # print(params[0].decode(encoding='utf-8'))
                self.download_file(connection, params[0].decode(encoding='utf-8'))

                connection.send(b'')
            elif command == b'time':
                connection.send(str(datetime.datetime.now().time()).encode(encoding='utf-8'))
            else:
                connection.send(b'unknown command, please try again')


    def closeConnection(self, connection):
        client = list(filter(lambda x: x[0] == connection, self.connections))[0]

        self.log('connection closed {}'.format(client[1]))
        self.connections.remove(client)
        try:
            client.send(b'connection closed press enter')
        except Exception as e:
            pass

    def serverStart(self):
        os.chdir(self.STORAGE_DIR) # изменяем текущий рабочий каталог
        self.log('server started')

        while True:
            try:
                conn, addr = self.clientWait()
                # LAST_IP = addr
                action = self.clientProcessing(connection=conn, addr=addr)
                if action == -1:
                    return

                self.closeConnection(conn)
            except ConnectionResetError as e:
                self.log(str(e))
                self.LAST_IP = addr[0]
                self.closeConnection(conn)
            except Exception as e:
                self.log(str(e))
                self.closeConnection(conn)

    def log(self, message):
        if self.log_file.closed:
            self.log_file = open(self.LOG_FILE, 'a')

        print('{}: {}'.format(datetime.datetime.now(), message))
        self.log_file.write('{}: {}\n'.format(datetime.datetime.now(), message))

    def stop(self):
        for conn, addr in self.connections:
            if not conn.close:
                conn.send(self.SERVER_STOPPED_MESSAGE)
                conn.close()
                self.log(f'{conn} closed by server')

        self.socket.close()

        self.log(f'socket closed')
        self.log(f'server stopped')

        self.log_file.close()

    def run(self):
        self.socket = self.socket if self.socket else self.createSocket()
        self.socketOpen()

        try:
            self.serverStart()
            self.stop()
        except KeyboardInterrupt as e:
            self.log(str(e))
            self.stop()

    def upload_file(self, sock, file_name, pos=0):

        self.PREV_COMMAND = 'U'
        print(pos)
        posit = int(pos)
        print(posit)
        # receive the file infos
        # receive using client socket, not server socket
        received = sock.recv(BUFFER_SIZE).decode()
        sock.send(b'Start')
        file_name, filesize = received.split(SEPARATOR)
        # print('Size: ', filesize)
        # remove absolute path if there is
        file_name = os.path.basename(file_name)
        self.PREV_FILE = file_name
        # convert to integer
        filesize = int(filesize)
        # start receiving the file from the socket
        # and writing to the file stream
        progress = tqdm.tqdm(range(filesize), f"Progress of {file_name}:", unit="B", unit_scale=True, unit_divisor=1024)
        # if(LAST_B!=0):
        #     progress.update(LAST_B)
        progress.update(posit)
        total_read = 0
        with open(file_name, "wb") as f:
            while True:
                # read 1024 bytes from the socket (receive)
                bytes_read = sock.recv(BUFFER_SIZE)
                if not bytes_read:
                    # nothing is received
                    # file transmitting is done
                    self.LAST_IP = addr[0]
                    break
                # write to the file the bytes we just received
                f.write(bytes_read)
                # LAST_B += len(bytes_read)
                # update the progress bar
                progress.update(len(bytes_read))
                total_read += len(bytes_read)
                if total_read == filesize:
                    progress.close()
                    print('All')
                    break
        # LAST_B = 0
        self.PREV_COMMAND = '-'
        self.PREV_FILE = '-'
        f.close()

    def download_file(self, connection, params, pos=0):
        # print('bb')
        # print(pos)
        posit = int(pos)
        # print(posit)
        self.PREV_COMMAND = 'D'
        name_string = params
        if not os.path.isfile(name_string):
            print('File does not exist')
            filesize = '-'
            connection.send(f"{name_string}{SEPARATOR}{filesize}".encode())
            return

        filesize = os.path.getsize(name_string)
        self.PREV_FILE = name_string
        # print(name_string)
        if pos==0:
            connection.send(f"{name_string}{SEPARATOR}{filesize}".encode())
        progress = tqdm.tqdm(range(filesize), f"Progress of {name_string}:", unit="B", unit_scale=True, unit_divisor=1024)

        # if(LAST_B!=0):
        #     progress.update(LAST_B)

        progress.update(posit)

        f = open(name_string, "rb")

        bytes_read = f.read()
        bytes_read = bytes_read[posit:]

        # ii = 0
        while len(bytes_read) >= BUFFER_SIZE:
            part = bytes_read[:BUFFER_SIZE]
            bytes_read = bytes_read[BUFFER_SIZE:]
            connection.send(part)
                # update the progress bar
            progress.update(len(part))

        if (len(bytes_read)) > 0:
            connection.send(bytes_read)
            # update the progress bar
            # LAST_B += len(bytes_read)
            progress.update(len(bytes_read))
        progress.close()

        print('All')
        # LAST_B = 0
        self.PREV_COMMAND = '-'
        self.PREV_FILE = '-'
        f.close()


# def startServer():
#     hostname = socket.gethostname()
#     ip_address = socket.gethostbyname(hostname)
#
#     sock = socket.socket()
#     sock.bind(('192.168.43.212', 54320))
#     sock.listen(MAX_QUERY_SIZE)
#
#     print('-' * 5, 'TCP Server v1.0 started', '-' * 5)
#     print('server ip address = {}'.format(ip_address))
#
#     conn, addr = sock.accept()
#     print('new connection addr : {}'.format(addr))
#
#     while True:
#         try:
#
#             while True:
#                 data = conn.recv(1024)
#
#                 if not data:
#                     break
#
#                 command, *params = data.split(b' ')
#
#                 if command == b'ping':
#                     data = b'pong'
#                 elif command == b'kill':
#                     conn.send(b'GoodBy!')
#                     conn.close()
#                     sock.close()
#                     return
#                 elif command == b'echo':
#                     data = b' '.join(params)
#                 elif command == b'send':
#                     if params[0] == b'file':
#                         with open(params[1].decode(encoding="utf-8"), 'ab') as file:
#                             file.write(params[2])
#                             while True:
#                                 data = conn.recv(1024)
#
#                                 if not data:
#                                     break
#                                 else:
#                                     file.write(data)
#
#                         print('receive file {}'.format(params[1].decode(encoding="utf-8")))
#                         data = b'ok'
#                 elif command == b'time':
#                     data = str(datetime.datetime.now().time()).encode(encoding='utf-8')
#                 else:
#                     print('receive from {} data: {}'.format(addr, data.decode(encoding="utf-8")))
#
#                 conn.send(data)
#
#             print('close connection on : {}'.format(conn))
#             conn.close()
#         except Exception as e:
#             print('SERVER ERROR {}'.format(e))
#         except KeyboardInterrupt as e:
#             if conn:
#                 conn.close()
#             sock.close()
#             print('SERVER STOP')
#             sys.exit(-1)


if __name__ == '__main__':
    server = TCPServer()
    server.run()
