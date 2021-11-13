def udp_recv(bytes_amount, timeout, datagrams_amount):
    global datagram_count_in 
    print('start ', datagram_count_in)
    # datagram_count_in_temp = datagram_count_in
    # error_flag = False
    exc_flag = False
    recv_flag = False
    udp_socket.settimeout(timeout)
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
            data_temp, addr = udp_socket.recvfrom(bytes_amount)
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
        udp_socket.sendto(str.encode(temp), addr)
    else:
        addr = None
    
    return data, addr, exc_flag
  
  
def udp_send(data, addr, bytes_amount, datagrams_amount):
    global datagram_count_out
    datagram_count_out_old = datagram_count_out
    print('start ', datagram_count_out)
    data_part = str()
    for i in range(datagrams_amount):
        temp = format(datagram_count_out, '05d')
        print('===iteration ', i)
        data_part = data[:bytes_amount]
        data_part = str.encode(temp + data_part)
        print(data_part)
        print(data)
        udp_socket.sendto(data_part, addr)
        data = data[bytes_amount:]
        # datagram_count_out += 1
        if datagram_count_out == 99999:
            datagram_count_out = 0
        else:
            datagram_count_out += 1

    seq_num = udp_socket.recvfrom(bytes_amount)
    
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
