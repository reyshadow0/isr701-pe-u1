
import argparse

import grpc

from proto import messaging_pb2
from proto import messaging_pb2_grpc


def enviar_mensaje(nodo, destino, timestamp, mensaje):
    with grpc.insecure_channel(destino) as canal:
        cliente = messaging_pb2_grpc.MessagingServiceStub(canal)

        solicitud = messaging_pb2.Request(
            sender=nodo,
            timestamp=timestamp,
            message=mensaje
        )

        try:
            respuesta = cliente.SendMessage(solicitud, timeout=5)

            print(f"Estado: {respuesta.status}")
            print(f"Receptor: {respuesta.receiver}")

        except grpc.RpcError as error:
            print(f"Error de comunicacion: {error.code().name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodo", required=True)
    parser.add_argument("--destino", required=True)
    parser.add_argument("--timestamp", type=int, default=0)
    parser.add_argument("--mensaje", required=True)

    args = parser.parse_args()

    enviar_mensaje(
        args.nodo,
        args.destino,
        args.timestamp,
        args.mensaje
    )
