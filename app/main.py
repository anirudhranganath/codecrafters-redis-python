import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # The return value includes:
    # 1. the client's connection (a `Socket` object)
    # 2. the client's address (a tuple of the IP address and port number)
    connection, _ = server_socket.accept()

    # Send the response to the client
    # The b prefix converts the string to a bytes object. 
    # +PONG\r\n is the string "PONG" encoded as a RESP simple string.
    # https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
    connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
