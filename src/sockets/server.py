
import argparse
import json
import socket
import threading
from src.comun.lamport import RelojLamport

bloqueo_consola = threading.Lock()


def atender_cliente(conexion, nodo, reloj_lamport):
    """Recibe un mensaje JSON y devuelve un acuse."""

    with conexion:
        conexion.settimeout(5)

        try:
            with conexion.makefile("rb") as entrada:
                datos = entrada.readline(1024 * 1024)

            if not datos or not datos.endswith(b"\n"):
                raise ValueError("Mensaje incompleto")

            mensaje = json.loads(datos.decode("utf-8"))

            if not isinstance(mensaje, dict):
                raise ValueError("El mensaje debe ser un objeto JSON")

            if not isinstance(mensaje.get("sender"), str):
                raise ValueError("sender debe ser texto")

            reloj = mensaje.get("timestamp")
            if type(reloj) is not int or reloj < 0:
                raise ValueError("timestamp debe ser un entero no negativo")

            if not isinstance(mensaje.get("message"), str):
                raise ValueError("message debe ser texto")
            reloj_lamport.recibir(reloj, mensaje["sender"])
            respuesta = {
                "status": "ok",
                "receiver": nodo
            }

        except (
            ValueError,
            UnicodeDecodeError,
            socket.timeout,
            OSError
        ) as error:
            respuesta = {
                "status": "error",
                "receiver": nodo,
                "detail": str(error)
            }

        try:
            datos_respuesta = json.dumps(respuesta) + "\n"
            conexion.sendall(datos_respuesta.encode("utf-8"))

            if respuesta["status"] == "ok":
                with bloqueo_consola:
                    print(
                        f"[{nodo}] Mensaje de {mensaje['sender']} "
                        f"(Lamport recibido: {reloj})"
                    )

        except OSError:
            with bloqueo_consola:
                print(f"[{nodo}] No se pudo enviar el acuse")


def iniciar_servidor(nodo, puerto, reloj_lamport):
    """Inicia el servidor TCP del nodo indicado."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
        servidor.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        servidor.bind(("127.0.0.1", puerto))
        servidor.listen()
        servidor.settimeout(0.5)

        print(f"[{nodo}] Servidor TCP en localhost:{puerto}")
        print("Presiona Ctrl+C para detenerlo")

        try:
            while True:
                try:
                    conexion, _ = servidor.accept()
                except socket.timeout:
                    pass
                else:
                    hilo = threading.Thread(
                        target=atender_cliente,
                        args=(conexion, nodo, reloj_lamport),
                        daemon=True
                    )
                    hilo.start()

        except KeyboardInterrupt:
            print(f"\n[{nodo}] Servidor detenido")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Servidor TCP para la practica PE-U1"
    )

    parser.add_argument("--nodo", required=True)
    parser.add_argument("--puerto", type=int, required=True)
    parser.add_argument("--corrida", required=True)
    parser.add_argument("--equipo", default=None)

    args = parser.parse_args()

    reloj_lamport = RelojLamport(
        nodo=args.nodo,
        mecanismo="sockets",
        corrida=args.corrida,
        equipo=args.equipo
    )

    try:
        iniciar_servidor(args.nodo, args.puerto, reloj_lamport)
    finally:
        reloj_lamport.cerrar()