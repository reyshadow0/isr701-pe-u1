"""Servidor gRPC de cada nodo, integrado con NodoComun."""

from concurrent import futures

import grpc

from proto import messaging_pb2, messaging_pb2_grpc


class MessagingServer(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, comun):
        self.comun = comun

    def SendMessage(self, request, context):
        if request.timestamp < 0:
            return messaging_pb2.Response(
                status="error",
                receiver=self.comun.nombre,
                detail="timestamp debe ser un entero no negativo",
            )

        # NodoComun registra el evento con el mismo reloj que usa el cliente.
        self.comun.al_recibir(request.sender, request.timestamp, request.message)
        return messaging_pb2.Response(status="ok", receiver=self.comun.nombre)


def iniciar_servidor_nodo(comun):
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        MessagingServer(comun), servidor
    )
    puerto = servidor.add_insecure_port(f"127.0.0.1:{comun.args.puerto}")
    if puerto == 0:
        raise OSError(f"No se pudo abrir el puerto {comun.args.puerto}")
    servidor.start()
    return servidor
