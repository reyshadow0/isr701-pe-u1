"""
Cada proceso ejecuta un servidor y un cliente, compartiendo
la lógica de Lamport y experimentación de NodoComun.
"""

import json
import socket
import sys

from src.comun.nodo import NodoComun, leer_argumentos
from src.sockets.server import iniciar_servidor_nodo

class TransporteTCP:
    """Mantiene una conexión TCP por destino."""

    def __init__(self, comun):
        self.comun = comun
        self.conexiones = {}

    def enviar(self, destino, timestamp, texto):
        if destino not in self.conexiones:

            def conectar():
                conexion = socket.create_connection(
                    ("127.0.0.1", self.comun.puerto(destino)),
                    timeout=5,
                )
                conexion.settimeout(15)
                return conexion

            conexion = self.comun.reintentar(conectar)
            self.conexiones[destino] = (
                conexion,
                conexion.makefile("rb"),
            )

        conexion, entrada = self.conexiones[destino]

        mensaje = {
            "sender": self.comun.nombre,
            "timestamp": timestamp,
            "message": texto,
        }

        conexion.sendall(
            (json.dumps(mensaje) + "\n").encode("utf-8")
        )

        datos = entrada.readline(1024 * 1024)

        if not datos or not datos.endswith(b"\n"):
            raise ConnectionError("Acuse TCP incompleto")

        respuesta = json.loads(datos.decode("utf-8"))

        if (
            respuesta.get("status") != "ok"
            or respuesta.get("receiver") != destino
        ):
            raise RuntimeError(f"Acuse TCP inválido: {respuesta}")

    def cerrar(self):
        for conexion, entrada in self.conexiones.values():
            entrada.close()
            conexion.close()


def main():
    args = leer_argumentos()
    comun = NodoComun(args, mecanismo="sockets")
    transporte = TransporteTCP(comun)
    servidor = None

    try:
        servidor = iniciar_servidor_nodo(comun)
        print(
            f"[{comun.nombre}] TCP escuchando en "
            f"127.0.0.1:{args.puerto}",
            flush=True,
        )

        return comun.correr(transporte.enviar)

    finally:
        transporte.cerrar()
        if servidor is not None:
            servidor.close()


if __name__ == "__main__":
    sys.exit(main())