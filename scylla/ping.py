from ._udp import UDPSocket


def ping():
    s = UDPSocket(50000)
    s.send("Foo")