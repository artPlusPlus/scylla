import socket


def get_tcp_host():
    for ip in socket.gethostbyname_ex(socket.gethostname())[-1]:
        if not ip.startswith('127'):
            return ip
    return None