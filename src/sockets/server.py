
import argparse
import json
import socket
import threading
from src.comun.lamport import RelojLamport

bloqueo_consola = threading.Lock()


def atender_cliente(conexion, nodo, reloj_lamport, comun=None):
    """Recibe varios mensajes JSON por una misma conexión TCP."""

    with conexion:
        conexion.settimeout(None)

        try:
            with conexion.makefile("rb") as entrada:
                while True:
                    datos = entrada.readline(1024 * 1024)

                    if not datos:
                        break
                    try:
                        if not datos.endswith(b"\n"):
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
                        if comun is not None:
                            comun.al_recibir(
                                mensaje["sender"],
                                reloj,
                                mensaje["message"]
                            )
                        else:
                            reloj_lamport.recibir(
                                reloj,
                                mensaje["sender"]
                            )
                        respuesta = {
                            "status": "ok",
                            "receiver": nodo
                        }

                    except (ValueError, UnicodeDecodeError) as error:
                        respuesta = {
                            "status": "error",
                            "receiver": nodo,
                            "detail": str(error)
                        }

                    datos_respuesta = json.dumps(respuesta) + "\n"
                    conexion.sendall(datos_respuesta.encode("utf-8"))

                    if respuesta["status"] == "ok":
                        with bloqueo_consola:
                            print(
                                f"[{nodo}] Mensaje de "
                                f"{mensaje['sender']} "
                                f"(Lamport recibido: {reloj})"
                            )

        except (socket.timeout, OSError) as error:
            with bloqueo_consola:
                print(f"[{nodo}] Conexión finalizada: {error}")


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

def iniciar_servidor_nodo(comun):
    """Inicia el servidor TCP para un nodo del experimento."""

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    try:
        servidor.bind(("127.0.0.1", comun.args.puerto))
        servidor.listen()
        servidor.settimeout(0.5)

        def aceptar_conexiones():
            while True:
                try:
                    conexion, _ = servidor.accept()

                except socket.timeout:
                    continue

                except OSError:
                    break

                hilo = threading.Thread(
                    target=atender_cliente,
                    args=(
                        conexion,
                        comun.nombre,
                        comun.reloj,
                        comun
                    ),
                    daemon=True
                )
                hilo.start()

        hilo_servidor = threading.Thread(
            target=aceptar_conexiones,
            daemon=True
        )
        hilo_servidor.start()

        return servidor

    except Exception:
        servidor.close()
        raise

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