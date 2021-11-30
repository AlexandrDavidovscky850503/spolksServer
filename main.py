import os
import socket
import datetime
import tqdm
import time

MAX_QUERY_SIZE = 1

SOCKET_PORT = 50018
SOCKET_HOST = 'localhost'
# SOCKET_HOST = '192.168.191.24'
CONNECTION_DATA = (SOCKET_HOST, SOCKET_PORT)
BUFFER_SIZE = 1024 * 32
SEPARATOR = "<SEPARATOR>"


# ============================================================
class TCPServer:
    SERVER_STOPPED_MESSAGE = b'SERVER STOPPED!'  # b-префикс означает bytes строковый литерал
    LOG_FILE = 'server_log_{}.log'

    RECEIVE_BUFFER_SIZE = 1024
    TIMEOUT = 60

    LOG_DIR = 'logs'
    STORAGE_DIR = 'storage'
    LAST_IP = '-'
    LAST_ID = 0
    PREV_COMMAND = '-'
    PREV_FILE = '-'
    progress = '-'

    upload_recieved = 0
    upload_file_size = 0

    def __init__(self, host='', port=SOCKET_PORT, max_client_count=MAX_QUERY_SIZE, sock=None,
                 log_file=None):  # конструктор класса
        self.max_client_count = max_client_count
        self.host = host
        self.port = port
        self.server_address = (self.host, self.port)
        self.socket = sock

        self.log_file = log_file
        self.startLogging()

        self.progressBarActivated = False

        self.connections = []

    def startLogging(self):
        cur_dir = os.path.abspath(os.path.curdir)  # Получить абсолютный путь файла или каталога
        storage_path = os.path.join(cur_dir,
                                    self.STORAGE_DIR)  # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *STORAGE_DIR
        log_path = os.path.join(cur_dir,
                                self.LOG_DIR)  # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *LOG_DIR

        if not os.path.exists(storage_path):
            os.mkdir(storage_path)  # создает каталог с именем storage_path

        if not os.path.exists(log_path):
            os.mkdir(log_path)  # создает каталог с именем log_path

        log_file = os.path.join(
            log_path,
            self.LOG_FILE.format(datetime.datetime.now().strftime('%d.%m.%Y__%H.%M.%S'))
        )

        if not self.log_file or self.log_file.closed:
            self.log_file = open(log_file, 'w', encoding="utf-8")

        self.log('server created')
        self.log('server storage path {}'.format(storage_path))
        self.log('server log path {}'.format(log_path))

        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)

        self.log('server ip address: port = {}:{}'.format(ip_address, self.port))
        self.log_file.close()
        self.LOG_FILE = log_file

    def socketOpen(self):
        self.socket.listen(
            self.max_client_count)  # подготавливает сокет для приема соединений, означает максимальное количество подключений, которые операционная система может поставить в очередь для этого сокета
        self.log('open socket for {} clients'.format(self.max_client_count))

    def createSocket(self):
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_STREAM)  # создать TCP-сокет семейства AF_INET типа потоковый сокет
        #  устанавливает значение опции сокета
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 8)
        # Время (в секундах) простоя (idle) соединения, по прошествии которого TCP начнёт отправлять проверочные пакеты (keepalive probes), если для сокета включён параметр SO_KEEPALIVE
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        # Время в секундах между отправками отдельных проверочных пакетов (keepalive probes).
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10)
        # Максимальное число проверок (keepalive probes) TCP, отправляемых перед сбросом соединения.

        sock.bind(self.server_address)  # bind () используется, когда сокет необходимо сделать сокетом сервера

        self.log('create socket {}'.format(sock))

        return sock

    def clientWait(self):
        # print("wait")
        conn, addr = self.socket.accept()  # Метод Socket.accept() принимает соединение. Сокет должен быть привязан к адресу и прослушивать соединения
        self.connections.append((conn, addr))
        self.log('new client connected {}'.format(addr))
        c_id = int(conn.recv(10))
        conn.send(b'Start')
        # print("c_id", c_id)
        return conn, addr, c_id

    def clientProcessing(self, connection, addr, c_id):
        hostname = socket.gethostname()

        while True:
            data = connection.recv(self.RECEIVE_BUFFER_SIZE)
            if not data:
                # print("not data")
                return

            command, *params = data.split(b' ')  # разбивает строку на части
            self.log('client {} send command {} with params {}'.format(addr, command, params))

            if command == b'ping':
                connection.send(b'ping')
            elif command == b'cont':
                if addr[0] == self.LAST_IP and self.LAST_ID == c_id:
                    if self.PREV_COMMAND == 'U':
                        self.upload_file(connection, self.PREV_FILE, 1)
                    elif self.PREV_COMMAND == 'D':
                        self.download_file(connection, self.PREV_FILE, params[0].decode(encoding='utf-8'))
                self.LAST_IP = '-'
                self.LAST_ID = -1
            elif command == b'help':
                connection.send(b'''help - to see list of commands
                ping - test that the server is alive
                kill - to stop server
                echo - to resend message to a client
                upload - to upload file on the server `upload file_name_on_your_machine.extension`
                download - to download file from a server `download file_name_on_server`
                time - get server time
                ''')
            elif command == b'kill':
                connection.send(b'GoodBy my friend!')
                return -1
            elif command == b'echo':
                connection.send(b' '.join(params))
            elif command == b'upload':
                self.upload_file(connection, params[0].decode(encoding='utf-8'))
            elif command == b'download':
                self.download_file(connection, params[0].decode(encoding='utf-8'))
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
        os.chdir(self.STORAGE_DIR)  # изменяем текущий рабочий каталог
        self.log('server started')

        while True:
            try:
                conn, addr, c_id = self.clientWait()
                action = self.clientProcessing(connection=conn, addr=addr, c_id=c_id)
                if action == -1:
                    return

                self.closeConnection(conn)
            except ConnectionResetError as e:
                self.log(str(e))
                self.LAST_IP = addr[0]
                self.LAST_ID = c_id
                if self.progressBarActivated:
                    self.progress.close()
                    self.progressBarActivated = False
                self.closeConnection(conn)
            except Exception as e:
                self.log(str(e))
                self.LAST_IP = addr[0]
                self.LAST_ID = c_id
                if self.progressBarActivated:
                    self.progress.close()
                    self.progressBarActivated = False
                self.closeConnection(conn)

    def log(self, message):
        if self.log_file.closed:
            self.log_file = open(self.LOG_FILE, 'a', encoding="utf-8")

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

    def recvall(self, sock, amount_to_read):
        n = 0
        data = bytearray()
        while n < amount_to_read:
            b = sock.recv(int(int(amount_to_read) - int(n)))
            if not b:
                # print('\nerror')
                # print(f"ERROR n in revall = {n}")
                raise ConnectionResetError("error in the recvall")
                return None
            n += len(b)
            data.extend(b)

        return data

    def upload_file(self, sock, file_name, pos=0):
        time.sleep(0.5)
        self.PREV_COMMAND = 'U'

        if pos == 0:
            mod = "wb"
            sock.send(b'Start')
            received = sock.recv(BUFFER_SIZE).decode()
            file_name, filesize = received.split(SEPARATOR)
            self.upload_file_size = int(filesize)
            self.PREV_FILE = file_name
        else:
            mod = "ab"
            msg = f'{str(self.upload_recieved)}'
            sock.send(bytes(msg, encoding='utf-8'))
            file_name = self.PREV_FILE
            filesize = self.upload_file_size

        file_name = os.path.basename(file_name)
        self.progress = tqdm.tqdm(range(int(filesize)), f"Progress of {file_name}:", unit="B", unit_scale=True,
                                  unit_divisor=1024)
        self.progressBarActivated = True
        self.progress.update(self.upload_recieved)
        if pos == 0:
            total_read = 0
            if int(filesize) >= BUFFER_SIZE:
                amount_to_read = BUFFER_SIZE
            else:
                amount_to_read = int(filesize)
        else:
            total_read = self.upload_recieved
            if int(filesize) - self.upload_recieved >= BUFFER_SIZE:
                amount_to_read = BUFFER_SIZE
            else:
                amount_to_read = int(filesize) - self.upload_recieved
        with open(file_name, mod) as f:
            while True:
                bytes_read = self.recvall(sock, amount_to_read)
                sock.send(b'Start')
                f.write(bytes_read)
                self.progress.update(len(bytes_read))
                total_read += len(bytes_read)
                self.upload_recieved = total_read
                if int(filesize) - total_read >= BUFFER_SIZE:
                    amount_to_read = BUFFER_SIZE
                else:
                    amount_to_read = int(filesize) - total_read
                if total_read == int(filesize):
                    self.progress.close()
                    self.progressBarActivated = False
                    print('All')
                    break
        self.PREV_COMMAND = '-'
        self.PREV_FILE = '-'
        self.upload_recieved = 0
        self.upload_file_size = 0
        f.close()

    def download_file(self, connection, params, pos=0):
        time.sleep(0.5)
        posit = int(pos)
        self.PREV_COMMAND = 'D'
        name_string = params
        if not os.path.isfile(name_string):
            print('File does not exist')
            filesize = '-'
            connection.send(f"{name_string}{SEPARATOR}{filesize}".encode())
            return

        filesize = os.path.getsize(name_string)
        self.PREV_FILE = name_string
        f = open(name_string, "rb")
        if pos == 0:
            connection.send(f"{name_string}{SEPARATOR}{filesize}".encode())
        else:
            self.progress.close()
            self.progressBarActivated = False
            f.seek(posit)
        self.progress = tqdm.tqdm(range(filesize), f"Progress of {name_string}:", unit="B", unit_scale=True,
                                  unit_divisor=1024)
        self.progressBarActivated = True
        self.progress.update(posit)

        read_amount = posit
        while 1:
            part = f.read(BUFFER_SIZE)
            connection.send(part)
            self.progress.update(len(part))
            read_amount += len(part)
            if read_amount == filesize:
                break

        self.progress.close()
        self.progressBarActivated = False
        print('All')
        self.PREV_COMMAND = '-'
        self.PREV_FILE = '-'
        f.close()


# ============================================================

# ============================= UDP START ===================================
clients_addr = []
waiting_clients = []
DOWNLOAD_PROGRESS = 0
OK_STATUS = 200
SERVER_ERROR = 500
UDP_BUFFER_SIZE = 32768
UDP_DATAGRAMS_AMOUNT = 15
LAST_CLIENT_ID = 0

datagram_count_in = 0
datagram_count_out = 0


# UDP_SERVER = '-'

def download(addr, file_name):
    print('=Download=')

    f = open(file_name, "rb+")

    size = int(os.path.getsize(file_name))
    total_size = 0

    print("File size:", size)

    send_data(addr, size)

    data_size_recv = int(get_data()[0])

    # send_data(addr, data_size_recv)

    f.seek(data_size_recv, 0)
    current_pos = data_size_recv

    progress = tqdm.tqdm(range(int(size)), f"Progress of {file_name}:", unit="B", unit_scale=True,
                         unit_divisor=1024)

    progress.update(total_size)
    while 1:
        try:
            if 1:
                data_file = f.read(UDP_BUFFER_SIZE * UDP_DATAGRAMS_AMOUNT)
                # server.sendto(data_file, addr)
                udp_send(data_file, addr, UDP_BUFFER_SIZE, UDP_DATAGRAMS_AMOUNT)

                current_pos = current_pos + UDP_BUFFER_SIZE * UDP_DATAGRAMS_AMOUNT
                f.seek(current_pos)
                total_size += len(data_file)
                # print('upd')
                progress.update(len(data_file))
                # print(total_size)
                if total_size == size:
                    break
        except Exception:
            current_pos = current_pos + UDP_BUFFER_SIZE * UDP_DATAGRAMS_AMOUNT
            total_size += len(data_file)
            if total_size == size:
                print('Ack for last portion was not received. Probably the client was disconnected')
            else:
                print('Client disconnected')
            break
            # f.close()
            # server.close()
            # progress.close()
            # os._exit(1)

    progress.close()
    print("END")
    if total_size == size:
        print("All")
    f.close()


def upload(addr, file_name):
    print('=File upload=')
    global DOWNLOAD_PROGRESS
    size = int(get_data()[0])
    total_size = 0
    print("size =", size)
    # print(size)
    send_data(addr, DOWNLOAD_PROGRESS)
    file_name = os.path.basename(file_name)

    f = open(file_name, "wb")

    # current_pos = data_size_recv
    current_pos = 0
    # print("=====================")
    i = 0

    progress = tqdm.tqdm(range(int(size)), f"Progress of {file_name}:", unit="B", unit_scale=True,
                         unit_divisor=1024)
    progress.update(total_size)

    while (1):
        try:
            data, address, a = udp_recv(UDP_BUFFER_SIZE + 5, 10.0, UDP_DATAGRAMS_AMOUNT)
            if data:
                # if data == b'EOF':
                #     break
                # else:
                if 1:
                    i += 1
                    f.seek(current_pos, 0)
                    f.write(data)
                    current_pos += len(data)
                total_size += len(data)
                progress.update(len(data))
                # print(total_size)
                if total_size == size:
                    break

            else:
                print("Client disconnected")
                return

        except Exception:
            print("Client disconnected")
            break
            # send_data(addr, "ERROR")
            # f.close()
            # server.close()
            # progress.close()
            # os._exit(1)
            # progress.close()
    progress.close()
    print("END")
    if size == total_size:
        print("\n" + file_name + " was uploaded")
    f.close()


def save_to_waiting_clients(addr, command, file_name, progress):
    waiting_clients.append(
        {
            'addr': addr[0],
            'command': command,
            'file_name': file_name,
            'progress': progress
        })


def get_data():
    data, address, a = udp_recv(UDP_BUFFER_SIZE, None, 1)
    data = data.decode('utf-8')
    return [data, address]


def send_data(addr, data):
    # server.sendto(str(data).encode('utf-8'), addr)
    print('send data', len(str(data).encode('utf-8')))
    udp_send(str(data).encode('utf-8'), addr, UDP_BUFFER_SIZE, 1)


def send_status(addr, request, status):
    message = str("" + request + " " + str(status)).encode('utf-8')
    print('send status', len(message))
    udp_send(message, addr, len(message), 1)
    # send_data(addr, message)


def handle_client_request(addr, request):
    data = request.split()
    command = data[0]

    if len(data) == 2:
        params = data[1]

    try:
        if command == "download":
            print(params)
            if os.path.isfile(params):  # ==
                send_status(addr, command, OK_STATUS)  # ==
                download(addr, params)
            else:
                send_status_and_message(addr, command, SERVER_ERROR, "No such file")
        elif command == "upload":
            print(params)
            # send_status(addr, command, OK_STATUS)  # ==
            upload(addr, params)
        elif command == "echo":

            # send_status(addr, command, OK_STATUS)
            echo(addr, params)

        elif command == "time":
            # send_status(addr, command, OK_STATUS)
            send_time(addr)

        elif command == "exit":
            # send_status(addr, command, OK_STATUS)
            exit_client(addr)
        elif command == "connect":
            # send_status(addr, command, OK_STATUS)
            connect_new_client(addr, params)
        else:
            print('Unknown command received!')
            send_status_and_message(addr, command, SERVER_ERROR, "Unknown command")
    except Exception:
        print('Unable to process the command')


def connect_new_client(addr, params):
    global LAST_CLIENT_ID
    add_client_address(addr)
    LAST_CLIENT_ID = int(params)


def exit_client(addr):
    clients_addr.remove(addr)


def send_time(addr):
    server_time = "Server time: " + str(datetime.datetime.now().time())[:19]
    udp_send(server_time.encode('utf-8'), addr=addr, bytes_amount=UDP_BUFFER_SIZE, datagrams_amount=1)
    # send_data(addr, server_time)


def echo(addr, body):
    udp_send(body.encode('utf-8'), addr=addr, bytes_amount=UDP_BUFFER_SIZE, datagrams_amount=1)
    # send_data(addr, body)


def send_status_and_message(addr, request, status, message):
    message = str("" + request + " " + str(status) + " " + message)
    send_data(addr, message)


def add_client_address(addr):
    if not addr in clients_addr:
        clients_addr.append(addr)
        print("Accepted client", addr)


def get_data_from_client():
    print('Waiting for command...')
    data, address, a = udp_recv(UDP_BUFFER_SIZE, None, 1, True)
    # print(data)
    # print(address)
    data = data.decode('utf-8')
    return [data, address]


def udp_send(data, addr, bytes_amount, datagrams_amount):
    global datagram_count_out
    fl = False
    datagram_count_out_begin = int(datagram_count_out)
    data_temp = bytes(data)
    i_temp = 0
    seq_num = (-1, '127.0.0.1')
    while True:
        # print('A2A2', datagram_count_out)
        for i in range(i_temp, datagrams_amount):
            temp = format(datagram_count_out, '05d').encode('utf-8')
            # print('===iteration ', i)
            # print('datagram_count_out', datagram_count_out)
            data_part = data[:bytes_amount]
            data_part = temp + data_part
            # if not data_part:
            #     print('Bla')

            server.sendto(data_part, addr)
            # print(datagram_count_out)
            data = data[bytes_amount:]
            # datagram_count_out += 1
            if datagram_count_out == 99999:
                datagram_count_out = 0
            else:
                datagram_count_out += 1
            # if i < datagrams_amount - 1:
            try:
                fl = False
                server.settimeout(0)
                seq_num = server.recvfrom(5)
                server.settimeout(None)
                fl = True
                break

            except Exception:
                server.settimeout(15)
                pass

        if not fl:
            # print('A0A0', datagram_count_out)
            server.settimeout(8)
            seq_num = server.recvfrom(UDP_BUFFER_SIZE + 5)
            if len(seq_num) > 5:
                raise Exception

            # print('A1A1', seq_num)
        fl = False
        server.settimeout(None)

        if datagram_count_out_begin + datagrams_amount > 99999:
            dd = datagram_count_out_begin + datagrams_amount - 100000
        else:
            dd = datagram_count_out_begin + datagrams_amount

        if datagram_count_out == int(seq_num[0]) and datagram_count_out == dd:
            # print('BBBB')
            # datagram_count_out = int(seq_num[0])
            # print('finish ', datagram_count_out)
            return True
        else:
            datagram_count_out = int(seq_num[0])
            # datagrams_amount = datagram_count_out - datagram_count_out_old
            if int(seq_num[0]) - datagram_count_out_begin < 0:
                i_temp = 100000 + int(seq_num[0]) - datagram_count_out_begin
            else:
                i_temp = int(seq_num[0]) - datagram_count_out_begin
            datagram_count_out_old = int(seq_num[0])
            data = bytes(data_temp[(datagram_count_out - datagram_count_out_begin) * bytes_amount:])

            # print('finish ', datagram_count_out)
            # return False, sent_amount
            continue


def udp_recv(bytes_amount, timeout, datagrams_amount, wait_flag = False):
    # print('a')
    global clients_addr
    global datagram_count_in
    global datagram_count_out
    # datagram_count_in_old = datagram_count_in
    datagram_count_in_begin = datagram_count_in

    exc_flag = False
    aaa = False
    # tim1 = timeout if timeout == None else 1
    # tim2 = timeout if timeout == None else 1
    new_client_flag = False
    data = bytes()
    counter = 0
    req = 0

    i_temp = 0
    addr = ('127.0.0.1', SOCKET_PORT - 1)

    while 1:
        i = i_temp
        while i < datagrams_amount:
            try:
                if aaa:
                    server.settimeout(timeout)
                else:
                    server.settimeout(0.2)
                # print(i)
                data_temp, addr = server.recvfrom(bytes_amount)
                # if len(data_temp) < bytes_amount:
                #     print(len(data_temp))
                #     input('a')
                #     raise Exception
                # print(data_temp)
                server.settimeout(None)

                # print('===iteration ', i)

                data_temp2 = data_temp.decode('utf-8')
                if data_temp2[0] == 'g':
                    # print('New client connected')
                    data_temp = data_temp[1:]
                    new_client_flag = True

                seq_num = int(data_temp[:5])

                if aaa and seq_num == req:
                    # print('B')
                    aaa = False
            except Exception:
                # print(f'bbbbbb', i)
                i += 1
                exc_flag = True
                if aaa and not wait_flag:
                    raise Exception
                # continue
                break

            # print(new_client_flag)
            # input('a')

            if not new_client_flag and datagram_count_in == seq_num:
                counter += 1
                data += bytes(data_temp[5:])
                if datagram_count_in == 99999:
                    datagram_count_in = 0
                else:
                    datagram_count_in += 1

            elif new_client_flag:
                # input('a')
                counter += 1
                datagram_count_in = seq_num + 1
                datagram_count_out = 0
                data += bytes(data_temp[5:])
            else:
                break

            i += 1

        if counter == datagrams_amount:
            # print('aaaaa1')
            temp = format(datagram_count_in, '05d')
            server.settimeout(None)
            server.sendto(str.encode(temp), addr)
            break
        else:
            # print('aaaaa3', datagram_count_in)
            aaa = True

            if datagram_count_in - datagram_count_in_begin < 0:
                i_temp = 100000 + datagram_count_in - datagram_count_in_begin
            else:
                i_temp = datagram_count_in - datagram_count_in_begin

            # datagram_count_in_old = datagram_count_in

            req = datagram_count_in
            temp = format(datagram_count_in, '05d')
            server.settimeout(None)
            server.sendto(str.encode(temp), addr)
            continue

    return data, addr, exc_flag


# ============================= UDP END =====================================

# if __name__ == '__main__':
print("1 - UDP Server\n2 - TCP Server")
# num = int(input("Введите число: "))
num = 0
while (1):
    num = input('Введите число: ')
    try:
        num = int(num)
        if num == 1 or num == 2:
            break
    except ValueError:
        pass
    print('Check your input!')
if num == 2:
    print("TCP server")
    server = TCPServer()
    server.run()
elif num == 1:
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('', SOCKET_PORT))
    print("UDP server : %s:%d(UDP)" % (SOCKET_HOST, SOCKET_PORT))

    # UDP_SERVER =
    os.chdir('storage')  # изменяем текущий рабочий каталог
    cur_dir = os.path.abspath(os.path.curdir)  # Получить абсолютный путь файла или каталога
    storage_path = os.path.join(cur_dir,
                                'storage')  # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *STORAGE_DIR
    if not os.path.exists(storage_path):
        os.mkdir(storage_path)  # создает каталог с именем storage_path
    time.sleep(1)
    while True:
        request, addr = get_data_from_client()

        # add_client_address(addr)
        print("get a command: ", request)
        handle_client_request(addr, request)
