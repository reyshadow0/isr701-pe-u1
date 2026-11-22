"""Nodo gRPC: servidor y cliente comparten un único NodoComun."""

import sys

import grpc

from proto import messaging_pb2, messaging_pb2_grpc
from src.comun.nodo import NodoComun, leer_argumentos
from src.grpc.server_grpc import iniciar_servidor_nodo


class TransporteGRPC:
    """Reutiliza un canal gRPC por destino durante toda la corrida."""

    def __init__(self, comun):
        self.comun = comun
        self.canales = {}

    def enviar(self, destino, timestamp, texto):
        if destino not in self.canales:
            def conectar():
                canal = grpc.insecure_channel(
                    f"127.0.0.1:{self.comun.puerto(destino)}"
                )
                try:
                    # Crear el canal no significa que el servidor ya esté activo.
                    grpc.channel_ready_future(canal).result(timeout=1)
                    stub = messaging_pb2_grpc.MessagingServiceStub(canal)
                    return canal, stub
                except Exception:
                    canal.close()
                    raise

            self.canales[destino] = self.comun.reintentar(conectar)

        _, cliente = self.canales[destino]
        solicitud = messaging_pb2.Request(
            sender=self.comun.nombre,
            timestamp=timestamp,
            message=texto,
        )
        respuesta = cliente.SendMessage(solicitud, timeout=15)

        if respuesta.status != "ok" or respuesta.receiver != destino:
            raise RuntimeError(
                f"Acuse gRPC inválido de {destino}: "
                f"status={respuesta.status}, receiver={respuesta.receiver}, "
                f"detail={respuesta.detail}"
            )

    def cerrar(self):
        for canal, _ in self.canales.values():
            canal.close()


def main():
    args = leer_argumentos()
    comun = NodoComun(args, mecanismo="grpc")
    transporte = TransporteGRPC(comun)
    servidor = None

    try:
        servidor = iniciar_servidor_nodo(comun)
        print(
            f"[{comun.nombre}] gRPC escuchando en "
            f"127.0.0.1:{args.puerto}",
            flush=True,
        )
        return comun.correr(transporte.enviar)
    finally:
        transporte.cerrar()
        if servidor is not None:
            servidor.stop(0).wait()


if __name__ == "__main__":
    sys.exit(main())
