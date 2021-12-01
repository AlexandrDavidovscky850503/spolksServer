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
    if os.path.isfile(file_name):
        send_data(addr, OK_STATUS)
    else:
        send_data(addr, SERVER_ERROR)
        return

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

def get_data():
    data, address, a = udp_recv(UDP_BUFFER_SIZE, None, 1)
    data = data.decode('utf-8')
    return [data, address]

def send_data(addr, data):
    # server.sendto(str(data).encode('utf-8'), addr)
    print('send data', len(str(data).encode('utf-8')))
    udp_send(str(data).encode('utf-8'), addr, UDP_BUFFER_SIZE, 1)

def handle_client_request(addr, request):
    data = request.split()
    command = data[0]

    if len(data) == 2:
        params = data[1]

    try:
        if command == "download":
            print('File name:', params)
            download(addr, params)
        elif command == "upload":
            print('File name', params)
            upload(addr, params)
        elif command == "echo":
            echo(addr, params)
        elif command == "time":
            send_time(addr)
        elif command == "exit":
            exit_client(addr)
        elif command == "connect":
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

print("1 - UDP Server\n2 - TCP Server")
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
