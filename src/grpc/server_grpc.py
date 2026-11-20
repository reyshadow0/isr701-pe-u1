
import argparse
from concurrent import futures

import grpc

from proto import messaging_pb2
from proto import messaging_pb2_grpc


class MessagingServer(messaging_pb2_grpc.MessagingServiceServicer):

    def __init__(self, nodo):
        self.nodo = nodo

    def SendMessage(self, request, context):
        print(f"[{self.nodo}] Mensaje de {request.sender}")
        print(f"Timestamp: {request.timestamp}")
        print(f"Contenido: {request.message}")

        return messaging_pb2.Response(
            status="ok",
            receiver=self.nodo
        )


def iniciar_servidor(nodo, puerto):
    servidor = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        MessagingServer(nodo), servidor
    )

    servidor.add_insecure_port(f"127.0.0.1:{puerto}")
    servidor.start()

    print(f"[{nodo}] Servidor gRPC en el puerto {puerto}")

    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        servidor.stop(0).wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodo", required=True)
    parser.add_argument("--puerto", type=int, required=True)

    args = parser.parse_args()
    iniciar_servidor(args.nodo, args.puerto)
