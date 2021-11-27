import socket
import select
from datetime import datetime
import sys
import os
import os.path
import time

STORAGE_DIR = 'storage'
IP = ''

PORT = 19001
BUFFER_SIZE = 1024

TIMEOUT = 20
OK_STATUS = 200
SERVER_ERROR = 500

def send_status_and_message(client, request, status, message):
    message = str("" + request + " " + str(status) + " " + message)
    client.send(message.encode('utf-8'))

def send_status(client, request, status):
    message = str("" + request + " " + str(status))
    client.send(message.encode('utf-8'))

def handle_client(client):
    if (client["is_closed"] == False):
        request = client['socket'].recv(BUFFER_SIZE).decode('utf-8')
        request = request.strip()
        if request != '':
            print("[*] Received: %s" %request)
            handle_client_request(client, request)

def echo(client, params):
    time.sleep(0.001)
    send_data(client, params)

def send_time(client):
    server_time = "Server time: " + str(datetime.now())[:19]
    send_data(client, server_time)

def exit_client(client):
    global inputs

    inputs.remove(client['socket'])
    clients_pool.remove(client)
    client['is_closed'] = True
    client['socket'].close()

def handle_client_request(client, request):
    data = request.split()
    command = data[0]

    if (len(data) == 2):
        params = data[1]

    if command == "download":
        if str(params) in files:
            send_status(client['socket'], command, OK_STATUS)
            download(client, params)
        else:
            send_status_and_message(client['socket'], command, SERVER_ERROR, "No such file")

    elif command == "echo":
        send_status(client['socket'], command, OK_STATUS)
        echo(client, params)

    elif command == "time":
        send_status(client['socket'], command, OK_STATUS)
        send_time(client)

    elif command == "exit":
        send_status(client['socket'], command, OK_STATUS)
        exit_client(client)
    else:
        send_status_and_message(client['socket'], command, SERVER_ERROR, "Unknown command")


def search_by_ip(list, ip):
    found_client = [element for element in list if element['ip'] == ip]
    return found_client[0] if len(found_client) > 0 else False

def search_by_socket(list, socket):
    found_client = [element for element in list if element['socket'] == socket]
    return found_client[0] if len(found_client) > 0 else False

def save_to_waiting_clients(ip, command, file_name, progress):
    waiting_clients.append(
        {
            'ip': ip,
            'command': command,
            'file_name': file_name,
            'progress': progress
        })

def handle_disconnect(client, command, file_name, progress):
    save_to_waiting_clients(client['ip'], command, file_name, progress)
    clients_pool.remove(client)
    inputs.remove(client['socket'])
    client['socket'].close()

    sys.stdout.flush()
    print("\nClient was disconnected")
    sys.stdout.flush()

def wait_ok(client):
    result = ''
    while ( result != "OK"):
        try:
            result = client['socket'].recv(2).decode('utf-8')
        except:
            pass

def send_ok(client):
    client['socket'].send("OK".encode('utf-8'))

def get_data(client):
    result = ''
    while (not result):
        try:
            result = client['socket'].recv(BUFFER_SIZE).decode('utf-8')
        except:
            pass
    return result

def send_data(client, data):
    client['socket'].send(str(data).encode('utf-8'))

def download(client, file_name):
    obj_download = "C:/Users/xiaom/PycharmProjects/serv2/storage/"+file_name
    obj_download.encode('unicode_escape')
    f = open(obj_download, "rb+")
    size = int(os.path.getsize(obj_download))

    send_data(client, size)

    wait_ok(client)

    waiting_client = search_by_ip(waiting_clients, client['ip'])
    if (len(waiting_clients) > 0 and waiting_client != False):
        waiting_clients.remove(waiting_client)

    data_size_recv = int(get_data(client))

    if (waiting_client):
        if (waiting_client['file_name'] == file_name and waiting_client['command'] == 'download'):
            data_size_recv = int(waiting_client['progress'])
            send_data(client, data_size_recv)
    else:
        send_data(client, data_size_recv)

    wait_ok(client)

    client['is_downloading'] = True
    client['file'] = f

    f.seek(data_size_recv, 0)

    print("Start download of", file_name)

    while (data_size_recv < size):
        try:
            data_file = f.read(BUFFER_SIZE)
            client['socket'].sendall(data_file)
            return
        except socket.error as e:
            f.close()
            handle_disconnect(client, "download", file_name, data_size_recv)
            client['is_closed'] = True
            return

        except KeyboardInterrupt:
            server.close()
            client.socket.close()
            os._exit(1)

def continue_download(client, received_data):
    data_size_recv = int(received_data)
    f = client['file']
    size = int(os.path.getsize(f.name))
    if (data_size_recv < size):
        f.seek(data_size_recv)
        try:
            data_file = f.read(BUFFER_SIZE)
            client['socket'].sendall(data_file)
        except socket.error as e:
            f.close()
            handle_disconnect(client, "download", f.name, data_size_recv)
            client['is_closed'] = True
            return

        except KeyboardInterrupt:
            server.close()
            client.socket.close()
            os._exit(1)
    else:
        print(f.name, "was downloaded")
        client['is_downloading'] = False
        f.close()


cur_dir = os.path.abspath(os.path.curdir)  # Получить абсолютный путь файла или каталога
storage_path = os.path.join(cur_dir,STORAGE_DIR)  # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *STORAGE_DIR
if not os.path.exists(storage_path):
    os.mkdir(storage_path)  # создает каталог с именем storage_path
files = os.listdir(storage_path)
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server.bind((IP, PORT))
server.listen(1)
server.setblocking(0)

print("Hello, listened on %s:%d" %(IP, PORT))

clients_pool = []
waiting_clients = []

inputs = [server]

client_ID = 0

while True:

    inputready,outputready,exceptready = select.select(inputs,[], inputs)

    for ready_socket in inputready:
        if ready_socket == server:
            client, client_info = server.accept()
            client.setblocking(0)

            client_ip = client_info[0]
            client_port = client_info[1]

            print("[*] Accepted connection from: %s:%d" % (client_ip, client_port))

            client_obj = {
                            "id": client_ID,
                            "socket": client,
                            "ip": client_ip,
                            "is_closed": False,
                            "port": client_port,
                            "is_downloading": False,
                            "file": ''
                        }

            clients_pool.append(client_obj)
            inputs.append(client)

            client_ID += 1

        else:
            try:
                request = ready_socket.recv(BUFFER_SIZE).decode('utf-8')
                found_client = search_by_socket(clients_pool, ready_socket)
                if found_client['is_downloading']:
                    continue_download(found_client, request)
                elif request:
                    request = request.strip()
                    if request != '':
                        print("[*] Received: %s" % request)
                        handle_client_request(found_client, request)
                else:
                    exit_client(found_client)
            except ConnectionResetError:
                found_client = search_by_socket(clients_pool, ready_socket)
                print("[*] Connection with: %s:%d is closed" % (found_client['ip'], found_client['port']))
                exit_client(found_client)
