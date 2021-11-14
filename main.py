import os
import socket
import datetime
import tqdm
import time

MAX_QUERY_SIZE = 1

SOCKET_PORT = 50015
SOCKET_HOST = '127.0.0.1'
CONNECTION_DATA = (SOCKET_HOST, SOCKET_PORT)
BUFFER_SIZE = 1024 * 32
SEPARATOR = "<SEPARATOR>"



#============================================================

#============================================================

#============================= UDP START ===================================
clients_addr = []
waiting_clients = []
OK_STATUS = 200
SERVER_ERROR = 500
UDP_BUFFER_SIZE = 1024
UDP_WINDOW_SIZE = 4096

datagram_count_in = 0
datagram_count_out = 0

# UDP_SERVER = '-'

def download(addr, file_name):
    global UDP_WINDOW_SIZE

    f = open(file_name, "rb+")

    size = int(os.path.getsize(file_name))
    total_size=0

    print("File size: %f" % (size))

    client_window = int(get_data()[0])

    if (UDP_WINDOW_SIZE > client_window):
        UDP_WINDOW_SIZE = client_window

    send_data(addr, UDP_WINDOW_SIZE)

    send_data(addr, size)

    data_size_recv = int(get_data()[0])

    waiting_client = search_by_addr(waiting_clients, addr) # ==
    if (len(waiting_clients) > 0 and waiting_client != False and waiting_client["file_name"] == file_name and
                waiting_client['command'] == 'download'):
        waiting_clients.remove(waiting_client)
        data_size_recv = int(waiting_client['progress'])

    send_data(addr, data_size_recv)

    f.seek(data_size_recv, 0)

    current_pos = data_size_recv

    print("current_pos = ")
    print(current_pos)
    progress = tqdm.tqdm(range(int(size)), f"Progress of {file_name}:", unit="B", unit_scale=True,
                         unit_divisor=1024)
    progress.update(total_size)
    while (1):
        try:
            if (current_pos >= size):
                # server.sendto(b"EOF", addr)
                udp_send("EOF", addr, 1024, 1)
                break
            else:
                data_file = f.read(UDP_BUFFER_SIZE)
                # server.sendto(data_file, addr)
                udp_send(data_file, addr, 1024, 1)
                
                current_pos = current_pos + UDP_BUFFER_SIZE
                f.seek(current_pos)
                total_size+=len(data_file)
                progress.update(len(data_file))
                # print(total_size)
                if total_size == size:
                    break

            client_window = client_window - UDP_BUFFER_SIZE
            if (client_window == 0):

                received_data = get_data()[0]
                client_window = UDP_WINDOW_SIZE

                if (received_data == "ERROR"):
                    handle_disconnect(addr, "download", file_name, data_size_recv)
                    break
                else:
                    data_size_recv = int(received_data)



        except KeyboardInterrupt:
            f.close()
            server.close()
            os._exit(1)
            progress.close()

    progress.close()
    print("END")
    if(total_size == size):
        print("All")
    f.close()

def save_to_waiting_clients(addr, command, file_name, progress):
    waiting_clients.append(
        {
            'addr': addr[0],
            'command': command,
            'file_name': file_name,
            'progress': progress
        })

def handle_disconnect(client, command, file_name, progress):
    save_to_waiting_clients(client, command, file_name, progress)
    time.sleep(1)
    print("lost connection")

def search_by_addr(list, addr):
    found_client = [element for element in list if element['addr'] == addr[0]]
    return found_client[0] if len(found_client) > 0 else False


def get_data():
    # data, address = server.recvfrom(UDP_BUFFER_SIZE)
    data, address, a = udp_recv(1024, None, 1)
    data = data.decode('utf-8')
    return [data, address]

def send_data(addr, data):
    # server.sendto(str(data).encode('utf-8'), addr)
    udp_send(str(data).encode('utf-8'), addr, 1024, 1)

def send_status(addr, request, status):
    message = str("" + request + " " + str(status)).encode('utf-8')
    udp_send(message, addr, 1024, 1)
    # send_data(addr, message)

def handle_client_request(addr, request):
    data = request.split()
    command = data[0]

    if (len(data) == 2):
        params = data[1]

    if (command == "download"):
        print(params)
        if (os.path.isfile(params)): #==
            send_status(addr, command, OK_STATUS) #==
            download(addr, params)
        else:
            # no_file = "File: " + params + " is not exist."
            send_status_and_message(addr, command, SERVER_ERROR, "No such file")


    elif (command == "echo"):
        # send_status(addr, command, OK_STATUS)
        echo(addr, params)

    elif (command == "time"):
        # send_status(addr, command, OK_STATUS)
        send_time(addr)

    elif (command == "exit"):
        # send_status(addr, command, OK_STATUS)
        exit_client(addr)

    else:
        print('bbbbb')
        send_status_and_message(addr, command, SERVER_ERROR, "Unknown command")


def exit_client(addr):
    clients_addr.remove(addr)

def send_time(addr):
    server_time = "Server time: " + str(datetime.datetime.now().time())[:19]
    udp_send(server_time.encode('utf-8'), addr=addr, bytes_amount=len(server_time), datagrams_amount=1)
    # send_data(addr, server_time)

def echo(addr, body):
    udp_send(body.encode('utf-8'), addr=addr, bytes_amount=len(body), datagrams_amount=1)
    # send_data(addr, body)

def send_status_and_message(addr, request, status, message):
    message = str("" + request + " " + str(status) + " " + message)
    send_data(addr, message)

def add_client_address(addr):
    if not addr in clients_addr:
        clients_addr.append(addr)
        print("Accepted client", addr)

def get_data_from_client():
    # data, address = server.recvfrom(UDP_BUFFER_SIZE)
    data, address, a = udp_recv(1024, None, 1)
    print(data)
    print(address)
    data = data.decode('utf-8')
    return [data, address]

def udp_send(data, addr, bytes_amount, datagrams_amount):
    global datagram_count_out
    datagram_count_out_old = datagram_count_out
    print('Send')
    print('start ', datagram_count_out)
    data_part = bytes()
    for i in range(datagrams_amount):
        temp = format(datagram_count_out, '05d').encode('utf-8')
        print('===iteration ', i)
        data_part = data[:bytes_amount]
        data_part = temp + data_part
        print(data_part)
        print(data)
        server.sendto(data_part, addr)
        data = data[bytes_amount:]
        # datagram_count_out += 1
        if datagram_count_out == 99999:
            datagram_count_out = 0
        else:
            datagram_count_out += 1

    seq_num = server.recvfrom(bytes_amount)
    
    seq_num_int = int(seq_num[0])
    print(seq_num_int)
    print('datagram_count_out_old ', datagram_count_out_old)
    print('datagrams_amount ', datagrams_amount)
    print('seq_num_int ', seq_num_int)
    print('datagram_count_out ', datagram_count_out)
    if 99999 - datagram_count_out_old < datagrams_amount and seq_num_int >= 0 and seq_num_int < datagrams_amount - (99999 - datagram_count_out_old):
        sent_amount = 99999 - datagrams_amount + 1 + seq_num_int
    else:
        sent_amount = seq_num_int - datagram_count_out_old

    if datagram_count_out == int(seq_num[0]):
        datagram_count_out = int(seq_num[0])
        print('finish ', datagram_count_out)
        return True, sent_amount
    else:
        print('finish ', datagram_count_out)
        return False, sent_amount


def udp_recv(bytes_amount, timeout, datagrams_amount):
    global datagram_count_in 
    print('Recv')
    print('start ', datagram_count_in)
    # datagram_count_in_temp = datagram_count_in
    # error_flag = False
    exc_flag = False
    recv_flag = False
    server.settimeout(timeout)
    data = bytes()

    recv_flags = []
    # seq_nums = []
    buffer = []
    addr = '0.0.0.0'
    
    for i in range(datagrams_amount):
        recv_flags.append(False)
        # seq_nums.append(0)
        buffer.append(bytes())

    for i in range(datagrams_amount):
        print('===iteration ', i)
        try:
            data_temp, addr = server.recvfrom(bytes_amount)
            recv_flag = True
            print(data_temp)
            seq_num_str = data_temp[:5]
            seq_num = int(seq_num_str)
            print('seq_num', seq_num)
        except Exception:
            print('bbbbbb')
            exc_flag = True
            break
        # if not error_flag:
        
        print('seq_num', seq_num)
        if 99999 - datagram_count_in < datagrams_amount and seq_num >= 0 and seq_num < datagrams_amount - (99999 - datagram_count_in) - 1:
            seq_num_temp = 99999 + 1 + seq_num
        else:
            seq_num_temp = seq_num
        print('datagram_count_in', datagram_count_in)
        print('datagrams_amount', datagrams_amount)

        if seq_num_temp >= datagram_count_in and seq_num_temp < datagram_count_in + datagrams_amount:
            recv_flags[seq_num_temp - datagram_count_in] = not recv_flags[seq_num_temp - datagram_count_in]

            # seq_nums[seq_num - datagram_count_in] = seq_num
            buffer[seq_num_temp - datagram_count_in] = bytes(data_temp[5:])


            # print(datagram_count_in_temp)
            # data += data_temp
        print(recv_flags)
        print(buffer)
    
    if all(b==True for b in recv_flags):
        for i in range(datagrams_amount):
            data += buffer[i]
        if datagram_count_in + datagrams_amount >= 99999:
            datagram_count_in = datagrams_amount - (99999 - datagram_count_in) - 1
        else:
            datagram_count_in += datagrams_amount
    else:
        for i in range(datagrams_amount):
            if recv_flags[i] == True:
                data += buffer[i]
                if datagram_count_in == 99999:
                    datagram_count_in = 0
                else:
                    datagram_count_in += 1
            else:
                break
        
    print('finish ', datagram_count_in)

    if recv_flag:
        temp = format(datagram_count_in, '05d')
        server.sendto(str.encode(temp), addr)
    else:
        addr = None
    
    return data, addr, exc_flag

#============================= UDP END =====================================

# if __name__ == '__main__':
print("1 - UDP Server\n2 - TCP Server")
num = int(input("Введите число: "))
if num == 2:
    print("TCP server")
    server = TCPServer()
    server.run()
elif num == 1:
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(CONNECTION_DATA)
    print("UDP server : %s:%d(UDP)" % (SOCKET_HOST, SOCKET_PORT))

    # UDP_SERVER =
    os.chdir('storage')  # изменяем текущий рабочий каталог
    cur_dir = os.path.abspath(os.path.curdir)  # Получить абсолютный путь файла или каталога
    storage_path = os.path.join(cur_dir, 'storage')  # правильно соединяет переданный путь cur_dir к одному или более компонентов пути *STORAGE_DIR
    if not os.path.exists(storage_path):
        os.mkdir(storage_path)  # создает каталог с именем storage_path
    time.sleep(1)
    while True:
        request, addr = get_data_from_client()

        add_client_address(addr)
        print("get a command: ", request)
        handle_client_request(addr, request)

        
