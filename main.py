import threading
import time
import socket
import datetime
import random
# import numpy as np

DOWNLOAD_SERVICE_PORT = 50001
UPLOAD_SERVICE_PORT = 50002
ECHO_SERVICE_PORT = 50003
TIME_SERVICE_PORT = 50004

available_sockets_num = []

# server_download_sock = None
# server_upload_sock = None
# server_echo_sock = None
# server_time_sock = None

LOCK_ECHO = threading.Lock()
LOCK_TIME = threading.Lock()
LOCK_DOWNLOAD = threading.Lock()
LOCK_UPLOAD = threading.Lock()

users_echo_info = []
users_time_info = []
users_download_info = []
users_upload_info = []

UDP_BUFFER_SIZE = 32768
UDP_DATAGRAMS_AMOUNT = 15

def create_sock(port_num):
    new_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    new_socket.bind(('', port_num))

    return new_socket







def get_socket_num():
    global available_sockets_num

    index = random.randint(0, len(available_sockets_num) - 1)

    sock_num = available_sockets_num[index]
    available_sockets_num.remove(sock_num)

    return sock_num


def return_released_socket_num(sock_num):
    available_sockets_num.append(sock_num)


def udp_recv_1(sock, user, bytes_amount, timeout, datagrams_amount, wait_flag = False):
    # print('a')
    addr = user['address']
    datagram_count_in = user['datagram_count_in']
    datagram_count_out = user['datagram_count_out']

    datagram_count_in_begin = datagram_count_in

    exc_flag = False
    aaa = False
    new_client_flag = False
    data = bytes()
    counter = 0
    req = 0

    i_temp = 0

    while 1:
        i = i_temp
        while i < datagrams_amount:
            try:
                if aaa:
                    sock.settimeout(timeout)
                else:
                    sock.settimeout(0.2)
                # print(i)
                data_temp, addr = sock.recvfrom(bytes_amount)

                sock.settimeout(None)

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
            sock.settimeout(None)
            sock.sendto(str.encode(temp), addr)
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
            sock.settimeout(None)
            sock.sendto(str.encode(temp), addr)
            continue

    user['datagram_count_in'] = datagram_count_in

    return data, user



def udp_recv_from_new_user(sock, bytes_amount):
    data = bytes()     
    sock.settimeout(None)
    data_temp, addr = sock.recvfrom(bytes_amount)

    print('data_temp', data_temp)

    # data_temp = data_temp.decode('utf-8')

    seq_num = int(data_temp[:5])

    if seq_num == 0:
        datagram_count_in = seq_num + 1
        data += bytes(data_temp[5:])

        temp = format(datagram_count_in, '05d')
        sock.settimeout(None)
        sock.sendto(str(temp).encode('utf-8'), addr)

        new_user = {
            'address' : addr,
            'datagram_count_in' : datagram_count_in,
            'datagram_count_out' : 0,
            'request' : data
        }

    return new_user


def echo_thread(user):
    global users_echo_info
    print(f'[E][{datetime.datetime.now()}] Echo thread started, amount of users remaining: {len(users_echo_info)}')
    
    dunamic_sock_num = get_socket_num()
    print(f'[E][{datetime.datetime.now()}] dunamic_sock_num retrieved: {dunamic_sock_num}')
    echo_sock = create_sock(dunamic_sock_num)
    ip_addr = user['address']
    print(f'[E][{datetime.datetime.now()}] Echo thread for user [{ip_addr}] was started!')
    # echo(user['address'], user['params'])
    request = user['request'].decode('utf-8')
    command, params = request.split(' ')
    # print('For', str(format(dunamic_sock_num, '05d') + params).encode('utf-8'))
    user['request'] = str(format(dunamic_sock_num, '05d') + params).encode('utf-8')
    user = udp_send(echo_sock, user, user['request'], bytes_amount=UDP_BUFFER_SIZE, datagrams_amount=1)
    echo_sock.close()
    return_released_socket_num(dunamic_sock_num)
    print(f'[E][{datetime.datetime.now()}] dunamic_sock_num returned: {dunamic_sock_num}')

    LOCK_ECHO.acquire(True)
    users_echo_info.remove(next(item for item in users_echo_info if item["address"] == user['address']))  
    print(f'[E][{datetime.datetime.now()}] Echo thread finished, amount of users remaining: {len(users_echo_info)}')
    LOCK_ECHO.release()



def udp_send(sock, user, data, bytes_amount, datagrams_amount):
    # global datagram_count_out
    addr = user['address']
    datagram_count_in = user['datagram_count_in']
    datagram_count_out = user['datagram_count_out']

    fl = False
    datagram_count_out_begin = int(datagram_count_out)
    data_temp = bytes(data)
    i_temp = 0
    seq_num = (-1, '127.0.0.1')
    while True:
        for i in range(i_temp, datagrams_amount):
            print('datagram_count_out', datagram_count_out)
            temp = format(datagram_count_out, '05d').encode('utf-8')
            data_part = data[:bytes_amount]
            data_part = temp + data_part
            # if not data_part:
            #     print('Bla')

            sock.sendto(data_part, addr)
            # print(datagram_count_out)
            data = data[bytes_amount:]

            if datagram_count_out == 99999:
                datagram_count_out = 0
            else:
                datagram_count_out += 1
            try:
                fl = False
                sock.settimeout(0)
                seq_num = sock.recvfrom(5)
                sock.settimeout(None)
                fl = True
                break

            except Exception:
                sock.settimeout(15)
                pass

        if not fl:
            sock.settimeout(8)
            seq_num = sock.recvfrom(UDP_BUFFER_SIZE + 5)
            if len(seq_num) > 5:
                raise Exception

        fl = False
        sock.settimeout(None)

        if datagram_count_out_begin + datagrams_amount > 99999:
            dd = datagram_count_out_begin + datagrams_amount - 100000
        else:
            dd = datagram_count_out_begin + datagrams_amount

        if datagram_count_out == int(seq_num[0]) and datagram_count_out == dd:
            user['datagram_count_in'] = datagram_count_in
            user['datagram_count_in'] = datagram_count_out
            return user
        else:
            datagram_count_out = int(seq_num[0])

            if int(seq_num[0]) - datagram_count_out_begin < 0:
                i_temp = 100000 + int(seq_num[0]) - datagram_count_out_begin
            else:
                i_temp = int(seq_num[0]) - datagram_count_out_begin

            data = bytes(data_temp[(datagram_count_out - datagram_count_out_begin) * bytes_amount:])

            continue


def download_thread(user):
    global users_download_info
    print(f'[D][{datetime.datetime.now()}] Download thread started, amount of users remaining: {len(users_download_info)}')

    dunamic_sock_num = get_socket_num()
    print(f'[D][{datetime.datetime.now()}] dunamic_sock_num retrieved: {dunamic_sock_num}')
    download_sock = create_sock(dunamic_sock_num)
    ip_addr = user['address']
    print(f'[D][{datetime.datetime.now()}] Download thread for user [{ip_addr}] was started!')

    request = user['request'].decode('utf-8')
    command, params = request.split(' ')

    # user['request'] = str(format(dunamic_sock_num, '05d') + params).encode('utf-8')
    # user = udp_send(download_sock, user, user['request'], bytes_amount=UDP_BUFFER_SIZE, datagrams_amount=1)
    # download_sock.close()
    # return_released_socket_num(dunamic_sock_num)
    # print(f'[D][{datetime.datetime.now()}] dunamic_sock_num returned: {dunamic_sock_num}')

    LOCK_DOWNLOAD.acquire(True)
    users_download_info.remove(next(item for item in users_download_info if item["address"] == user['address']))  
    print(f'[D][{datetime.datetime.now()}] Download thread finished, amount of users remaining: {len(users_download_info)}')
    LOCK_DOWNLOAD.release()

def download_service_thread(name):
    global users_download_info
    # users_download_info = []
    print('[DS] Download service thread started!')

    server_download_sock = create_sock(DOWNLOAD_SERVICE_PORT)

    while 1:
        print(f'[DS][{datetime.datetime.now()}] Waiting for command')
        new_user = udp_recv_from_new_user(server_download_sock, UDP_BUFFER_SIZE)

        LOCK_DOWNLOAD.acquire(True)
        users_download_info.append(new_user)
        LOCK_DOWNLOAD.release()

        thread = threading.Thread(target=download_thread, args=(new_user,))
        thread.start()
    
        request = new_user['request'].decode('utf-8')
        print(f'[DS][{datetime.datetime.now()}] get a command: {request}')

    print('[DS] End of download service thread!')


def upload_service_thread(name):
    users_upload_info = []

    print('[US] Upload service thread started!!')

    server_upload_sock = create_sock(UPLOAD_SERVICE_PORT)

    time.sleep(5)

    print('[US] End of upload service thread!')


def echo_service_thread(name):
    global users_echo_info

    print('[ES] Echo service thread started!')

    server_echo_sock = create_sock(ECHO_SERVICE_PORT)

    while 1:
        print(f'[ES][{datetime.datetime.now()}] Waiting for command')
        new_user = udp_recv_from_new_user(server_echo_sock, UDP_BUFFER_SIZE)

        request = new_user['request'].decode('utf-8')

        LOCK_ECHO.acquire(True)
        users_echo_info.append(new_user)
        LOCK_ECHO.release()

        thread = threading.Thread(target=echo_thread, args=(new_user,))
        thread.start()
          
        print(f'[ES][{datetime.datetime.now()}] get a command: {request}')

    print('[ES] End of echo service thread!')

def time_thread(user):
    global users_time_info
    print(f'[T][{datetime.datetime.now()}] Time thread started, amount of users remaining: {len(users_time_info)}')
    
    dunamic_sock_num = get_socket_num()
    print(f'[T][{datetime.datetime.now()}] dunamic_sock_num retrieved: {dunamic_sock_num}')
    time_sock = create_sock(dunamic_sock_num)
    ip_addr = user['address']
    print(f'[T][{datetime.datetime.now()}] Time thread for user [{ip_addr}] was started!')

    params = str(datetime.datetime.now().time())[:19]

    print('For', str(format(dunamic_sock_num, '05d') + params).encode('utf-8'))
    user['request'] = str(format(dunamic_sock_num, '05d') + params).encode('utf-8')
    user = udp_send(time_sock, user, user['request'], bytes_amount=UDP_BUFFER_SIZE, datagrams_amount=1)
    time_sock.close()
    return_released_socket_num(dunamic_sock_num)
    print(f'[T][{datetime.datetime.now()}] dunamic_sock_num returned: {dunamic_sock_num}')

    LOCK_TIME.acquire(True)
    users_time_info.remove(next(item for item in users_time_info if item["address"] == user['address']))  
    print(f'[T][{datetime.datetime.now()}] Time thread finished, amount of users remaining: {len(users_time_info)}')
    LOCK_TIME.release()

def time_service_thread(name):
    global users_time_info
    
    print('[TS] Time service thread started!')

    server_time_sock = create_sock(TIME_SERVICE_PORT)

    while 1:
        print(f'[TS][{datetime.datetime.now()}] Waiting for command')
        new_user = udp_recv_from_new_user(server_time_sock, UDP_BUFFER_SIZE)

        request = new_user['request'].decode('utf-8')

        LOCK_TIME.acquire(True)
        users_time_info.append(new_user)
        LOCK_TIME.release()

        thread = threading.Thread(target=time_thread, args=(new_user,))
        thread.start()
          
        print(f'[TS][{datetime.datetime.now()}] get a command: {request}')

    print('[TS] End of time service thread!')



########################################### MAIN ###########################################
print('================================================================')
print('UDP Server!')
print('Services: \n1. Download File (PORT 50001)\
                \n2. Upload File (PORT 50002)\
                \n3. Echo (PORT 50003)\
                \n4. Get Time (PORT 50004)')

for i in range(100):
    available_sockets_num.append(50050 + i)
                
thread1 = threading.Thread(target=download_service_thread, args='1')
thread1.start()
thread2 = threading.Thread(target=upload_service_thread, args='2')
thread2.start()
thread3 = threading.Thread(target=echo_service_thread, args='3')
thread3.start()
thread4 = threading.Thread(target=time_service_thread, args='4')
thread4.start()

print('================================================================')
print('[MAIN Thread] Service threads were started! Print [exit] to exit')
print('================================================================')

# while(1):
#     exit_input = input()
#     if exit_input == 'exit':
#         break
#     print('[MAIN Thread] Check your input!')
