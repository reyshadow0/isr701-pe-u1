"""Corre el experimento completo de la PE-U1 con sockets y gRPC.

Uso (desde la raíz del repo):
    python -m src.experimento.correr_experimento --dry-run    # muestra el plan, no corre nada
    python -m src.experimento.correr_experimento              # corrida oficial
    python -m src.experimento.correr_experimento --solo latencia
    python -m src.experimento.correr_experimento --solo-juntar  # solo arma latency_*.csv

Qué corre:
  * Latencia secuencial: un cliente (nodo-B) manda 100 mensajes a un servidor (nodo-A).
  * Latencia concurrente: tres clientes (nodo-B, nodo-C, nodo-D) mandan a la vez a nodo-A.
    Las dos con 100 bytes y con 10 KB, una corrida de calentamiento (r0) que el
    análisis descarta y 5 repeticiones (r1..r5).
  * Intercambios de Lamport: 20 mensajes por corrida con 2, 3 y 4 nodos, 5 repeticiones.

En cada repetición se corren los dos mecanismos uno detrás del otro, y el que va
primero se alterna (r0: sockets primero, r1: gRPC primero, ...), para que ninguno
saque ventaja siempre por caché, por la red de loopback o por el sistema.

Cada corrida tiene una etiqueta, por ejemplo "n3-r1" o "s10240-r2", y TODOS los
nodos de esa corrida reciben la misma con --corrida. Así lamport.py arma el mismo
id de corrida (<equipo>_<etiqueta>) en todos los CSV y se pueden emparejar.

Al terminar, junta los CSV de latencia de cada cliente en data/latency_sockets.csv
y data/latency_grpc.csv. Los registros de pantalla de cada proceso van a logs/.

=====================================================================
INTERFAZ DE LOS NODOS (lo que programan Steffanny y Luis)
=====================================================================
El programa de cada nodo es src/sockets/client.py y src/grpc/client_grpc.py.
Un nodo es UN proceso: arranca su servidor en un hilo, usa el mismo programa
como cliente y tiene UN solo RelojLamport (src/comun/lamport.py) para los dos.

    python -m src.sockets.client --nodo nodo-A --puerto 5000 --corrida n3-r1
        --equipo <equipo> --pares nodo-A=5000,nodo-B=5001,nodo-C=5002 --modo <modo> ...

  --pares   todos los nodos de la corrida, en orden. Ese orden define la ronda:
            cada nodo le envía a su sucesor y el último al primero.
  --equipo  se le pasa tal cual a RelojLamport(equipo=...).
  Sale con código 0 si todo salió bien y con 1 si algo falló (conexión, plazo, etc.).

Reglas comunes:
  1. Primero abre su servidor en 127.0.0.1:<puerto> y recién después envía.
  2. Para conectarse a otro nodo reintenta cada 0,1 s hasta 10 s; si no responde,
     sale con código 1.
  3. Abre UNA sola conexión TCP (sockets) o UN solo canal (gRPC) por cada destino
     y hace por ahí todos sus envíos. En sockets, cada mensaje es una línea JSON
     terminada en "\n" y el servidor responde un acuse por línea, sin cerrar la
     conexión. (La hipótesis supone que el saludo de TCP y de HTTP/2 queda fuera.)
  4. Cada envío llama a reloj.enviar(destino) y manda ese valor en timestamp; cada
     recepción llama a reloj.recibir(timestamp, origen). El acuse no es evento.
  5. El campo message lleva el contenido; su largo en bytes es el tamaño del mensaje.

--modo servidor
    Solo atiende. Este script lo cierra al final de la corrida.

--modo lamport --mensajes 20
    20 mensajes EN TOTAL por corrida (no por nodo), en dos fichas que dan vueltas
    a la ronda. Con los nodos N0..N(n-1) en el orden de --pares:
      * La ficha 1 sale de N0 y la ficha 2 sale de N(n//2): con 2 nodos, A y B;
        con 3, A y B; con 4, A y C. Cada ficha hace 10 saltos (mensajes).
      * El salto k de una ficha que salió de Ns va de N(s+k-1) a N(s+k), módulo n.
      * message = "ficha=<1|2> salto=<k>". Al recibir el salto k < 10, el nodo
        reenvía esa ficha a su sucesor con salto k+1. El salto 10 no se reenvía.
      * Los dos nodos que arrancan envían su salto 1 ANTES de procesar cualquier
        mensaje recibido. Para eso el servidor encola lo que llega (y responde el
        acuse enseguida) y el hilo principal llama a reloj.recibir y reenvía en
        orden de llegada. Así los dos primeros envíos salen con reloj 1 y son
        concurrentes.
      * Fin: con n, el orden de --pares y los 10 saltos de cada ficha, cada nodo
        calcula cuántos mensajes le toca enviar y recibir. Termina (código 0)
        cuando envió y recibió todos los suyos. Si en 30 s no lo logra, sale con 1.

--modo latencia --destino nodo-A --envios 100 --tamano 100 --escenario secuencial
    Manda --envios mensajes de --tamano bytes a --destino, uno detrás del otro, y
    mide cada ida y vuelta con time.perf_counter() (desde antes de enviar hasta
    recibir el acuse). Escribe SU PROPIO archivo, uno por cliente y corrida:
        data/latencia_<mecanismo>_<nodo>_<equipo>_<etiqueta>.csv
    con las columnas (en este orden):
        mecanismo, escenario, tamano_bytes, corrida, nodo, envio, rtt_ms, hora_real
    corrida = <equipo>_<etiqueta> (igual que en los eventos), nodo = el cliente,
    envio = 1..100, rtt_ms con 3 decimales, hora_real = time.time() con 6 decimales.
    Si el archivo ya existe, no arranca (como lamport.py).
"""

import argparse
import csv
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.experimento import condiciones

MECANISMOS = ("sockets", "grpc")
MODULOS = {"sockets": "src.sockets.client", "grpc": "src.grpc.client_grpc"}
PUERTOS = {"nodo-A": 5000, "nodo-B": 5001, "nodo-C": 5002, "nodo-D": 5003}
TAMANOS = (100, 10240)              # bytes del campo message: ~100 B y 10 KB
REPETICIONES = 5                    # r1..r5; r0 es el calentamiento
ENVIOS = 100
MENSAJES_LAMPORT = 20
NODOS_LAMPORT = (2, 3, 4)
SERVIDOR = "nodo-A"
CLIENTES = {"secuencial": ["nodo-B"], "concurrente": ["nodo-B", "nodo-C", "nodo-D"]}
PREFIJO = {"secuencial": "s", "concurrente": "c"}
COLUMNAS_LATENCIA = ["mecanismo", "escenario", "tamano_bytes", "corrida", "nodo",
                     "envio", "rtt_ms", "hora_real"]


@dataclass
class Corrida:
    etiqueta: str           # igual para todos los nodos de la corrida
    mecanismo: str
    tipo: str               # "latencia" o "lamport"
    escenario: str          # "secuencial", "concurrente" o "intercambios"
    nodos: list             # todos los nodos que participan
    repeticion: int         # 0 = calentamiento
    tamano: int = 0         # solo en latencia


def orden_mecanismos(repeticion, mecanismos=MECANISMOS):
    """Repeticiones pares: en el orden dado; impares: al revés."""
    return tuple(mecanismos) if repeticion % 2 == 0 else tuple(reversed(mecanismos))


def armar_plan(solo=None, repeticiones=REPETICIONES, mecanismos=MECANISMOS):
    """Devuelve la lista de corridas en el orden en que se ejecutan."""
    plan = []
    if solo in (None, "latencia"):
        for escenario in ("secuencial", "concurrente"):
            for tamano in TAMANOS:
                for rep in range(repeticiones + 1):          # 0 = calentamiento
                    for mec in orden_mecanismos(rep, mecanismos):
                        plan.append(Corrida(
                            etiqueta=f"{PREFIJO[escenario]}{tamano}-r{rep}",
                            mecanismo=mec, tipo="latencia", escenario=escenario,
                            nodos=[SERVIDOR] + CLIENTES[escenario],
                            repeticion=rep, tamano=tamano))
    if solo in (None, "lamport"):
        for n in NODOS_LAMPORT:
            for rep in range(1, repeticiones + 1):
                for mec in orden_mecanismos(rep, mecanismos):
                    plan.append(Corrida(
                        etiqueta=f"n{n}-r{rep}", mecanismo=mec, tipo="lamport",
                        escenario="intercambios", nodos=list(PUERTOS)[:n],
                        repeticion=rep))
    return plan


def comandos(corrida, equipo, modulos=MODULOS):
    """Arma el comando de cada nodo. Devuelve (servidores, clientes).

    Los servidores arrancan primero y este script los cierra al final;
    a los clientes se los espera hasta que terminen.
    """
    pares = ",".join(f"{n}={PUERTOS[n]}" for n in corrida.nodos)

    def base(nodo, modo):
        return [sys.executable, "-m", modulos[corrida.mecanismo],
                "--nodo", nodo, "--puerto", str(PUERTOS[nodo]),
                "--corrida", corrida.etiqueta, "--equipo", equipo,
                "--pares", pares, "--modo", modo]

    if corrida.tipo == "lamport":
        # Todos los nodos atienden y envían; ninguno es "solo servidor"
        clientes = {n: base(n, "lamport") + ["--mensajes", str(MENSAJES_LAMPORT)]
                    for n in corrida.nodos}
        return {}, clientes

    servidores = {SERVIDOR: base(SERVIDOR, "servidor")}
    clientes = {n: base(n, "latencia") + ["--destino", SERVIDOR,
                                           "--envios", str(ENVIOS),
                                           "--tamano", str(corrida.tamano),
                                           "--escenario", corrida.escenario]
                for n in corrida.nodos if n != SERVIDOR}
    return servidores, clientes


def puerto_ocupado(puerto):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", puerto)) == 0


def esperar_puerto(puerto, abierto, limite):
    """Espera hasta que el puerto quede abierto (o cerrado). Devuelve True si se cumplió."""
    fin = time.monotonic() + limite
    while time.monotonic() < fin:
        if puerto_ocupado(puerto) == abierto:
            return True
        time.sleep(0.1)
    return False


def cerrar(procesos):
    for p in procesos.values():
        if p.poll() is None:
            p.terminate()
    for p in procesos.values():
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def ejecutar(corrida, equipo, carpeta_logs, limite, modulos=MODULOS, datos="data"):
    """Corre una corrida. Devuelve (estado, detalle)."""
    servidores, clientes = comandos(corrida, equipo, modulos)

    for nodo in corrida.nodos:
        if not esperar_puerto(PUERTOS[nodo], abierto=False, limite=5):
            return "error", f"el puerto {PUERTOS[nodo]} de {nodo} sigue ocupado"

    abiertos = []
    lanzados = {}

    def lanzar(nodo, cmd):
        log = open(carpeta_logs / f"{corrida.mecanismo}_{corrida.etiqueta}_{nodo}.log",
                   "w", encoding="utf-8")
        abiertos.append(log)
        return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)

    try:
        servs = {n: lanzar(n, c) for n, c in servidores.items()}
        lanzados.update(servs)
        for nodo, p in servs.items():
            if not esperar_puerto(PUERTOS[nodo], abierto=True, limite=10) or p.poll() is not None:
                return "error", f"{nodo} no abrió el puerto {PUERTOS[nodo]}"

        clis = {n: lanzar(n, c) for n, c in clientes.items()}   # arrancan casi a la vez
        lanzados.update(clis)
        fin = time.monotonic() + limite
        for nodo, p in clis.items():
            try:
                p.wait(timeout=max(0.1, fin - time.monotonic()))
            except subprocess.TimeoutExpired:
                return "timeout", f"{nodo} no terminó en {limite} s"
        fallidos = [f"{n} (código {p.returncode})" for n, p in clis.items() if p.returncode != 0]
        if fallidos:
            return "error", "falló " + ", ".join(fallidos)
        caidos = [n for n, p in servs.items() if p.poll() is not None]
        if caidos:
            return "error", "el servidor se cerró antes de tiempo: " + ", ".join(caidos)
        return revisar_archivos(corrida, equipo, datos)
    finally:
        cerrar(lanzados)
        for log in abiertos:
            log.close()


def revisar_archivos(corrida, equipo, datos):
    """Comprueba que la corrida dejó los CSV que pide la interfaz."""
    faltan = []
    for nodo in corrida.nodos:
        ruta = Path(datos) / f"eventos_{corrida.mecanismo}_{nodo}_{equipo}_{corrida.etiqueta}.csv"
        if not ruta.exists():
            faltan.append(ruta.name)
    if corrida.tipo == "latencia":
        for nodo in CLIENTES[corrida.escenario]:
            ruta = Path(datos) / f"latencia_{corrida.mecanismo}_{nodo}_{equipo}_{corrida.etiqueta}.csv"
            if not ruta.exists():
                faltan.append(ruta.name)
                continue
            with open(ruta, newline="", encoding="utf-8") as f:
                filas = sum(1 for _ in f) - 1          # sin el encabezado
            if filas != ENVIOS:
                return "error", f"{ruta.name} tiene {filas} envíos y no {ENVIOS}"
    if faltan:
        return "error", "faltan " + ", ".join(faltan)
    return "ok", ""


def archivos_existentes(plan, equipo, carpeta):
    """CSV de eventos o de latencia que ya existen para las etiquetas del plan.

    Los nodos no pisan un archivo que ya existe, así que es mejor avisar antes de empezar.
    """
    existentes = []
    for c in plan:
        for prefijo in ("eventos", "latencia"):
            patron = f"{prefijo}_{c.mecanismo}_*_{equipo}_{c.etiqueta}.csv"
            existentes += sorted(Path(carpeta).glob(patron))
    return existentes


def juntar_latencia(carpeta="data"):
    """Junta los CSV de latencia de cada cliente en latency_<mecanismo>.csv.

    Copia las filas tal cual, en orden de nombre de archivo y luego de envío. Los
    archivos de cada cliente se quedan como están: son los datos crudos.
    Devuelve {mecanismo: (ruta, filas)}.
    """
    carpeta = Path(carpeta)
    resultado = {}
    for mec in MECANISMOS:
        archivos = sorted(carpeta.glob(f"latencia_{mec}_*.csv"))
        if not archivos:
            continue
        ruta = carpeta / f"latency_{mec}.csv"
        filas = 0
        with open(ruta, "w", newline="", encoding="utf-8") as salida:
            escritor = csv.writer(salida)
            escritor.writerow(COLUMNAS_LATENCIA)
            for archivo in archivos:
                with open(archivo, newline="", encoding="utf-8") as f:
                    lector = csv.reader(f)
                    if next(lector, None) != COLUMNAS_LATENCIA:
                        raise ValueError(f"{archivo}: las columnas no son {COLUMNAS_LATENCIA}")
                    for fila in lector:
                        escritor.writerow(fila)
                        filas += 1
        resultado[mec] = (ruta, filas)
    return resultado


def main():
    parser = argparse.ArgumentParser(description="Corre el experimento de sockets y gRPC.")
    parser.add_argument("--dry-run", action="store_true", help="solo muestra el plan y los comandos")
    parser.add_argument("--solo", choices=["latencia", "lamport"], help="corre solo una parte")
    parser.add_argument("--mecanismos", nargs="+", choices=MECANISMOS, default=list(MECANISMOS))
    parser.add_argument("--repeticiones", type=int, default=REPETICIONES)
    parser.add_argument("--equipo", default=socket.gethostname(),
                        help="nombre que va en los CSV (por defecto, el de la máquina)")
    parser.add_argument("--timeout", type=float, default=120, help="segundos máximos por corrida")
    parser.add_argument("--seguir", action="store_true", help="no detenerse si una corrida falla")
    parser.add_argument("--modulo-sockets", default=MODULOS["sockets"])
    parser.add_argument("--modulo-grpc", default=MODULOS["grpc"])
    parser.add_argument("--datos", default="data", help="carpeta de los CSV")
    parser.add_argument("--logs", default="logs", help="carpeta de los registros de cada proceso")
    parser.add_argument("--solo-juntar", action="store_true",
                        help="no corre nada: solo junta los CSV de latencia")
    args = parser.parse_args()

    if args.solo_juntar:
        for mec, (ruta, filas) in juntar_latencia(args.datos).items():
            print(f"{ruta}: {filas} filas")
        return

    modulos = {"sockets": args.modulo_sockets, "grpc": args.modulo_grpc}
    plan = armar_plan(args.solo, args.repeticiones, args.mecanismos)
    print(f"Plan: {len(plan)} corridas, equipo {args.equipo}")

    if args.dry_run:
        for i, c in enumerate(plan, 1):
            servidores, clientes = comandos(c, args.equipo, modulos)
            print(f"\n[{i}] {c.mecanismo} {c.etiqueta} ({c.escenario})")
            for nodo, cmd in {**servidores, **clientes}.items():
                print("   ", " ".join(cmd[1:]))
        return

    existentes = archivos_existentes(plan, args.equipo, args.datos)
    if existentes:
        sys.exit(f"Ya hay CSV de eventos para estas corridas (p. ej. {existentes[0]}). "
                 "Muévelos o usa otro --equipo antes de repetir.")

    datos = Path(args.datos)
    datos.mkdir(parents=True, exist_ok=True)
    carpeta_logs = Path(args.logs)          # fuera de data/: ahí solo van CSV y SHA256SUMS
    carpeta_logs.mkdir(parents=True, exist_ok=True)

    # Las condiciones de la máquina quedan junto a los datos de la corrida
    # El equipo va tal cual, igual que en los CSV de eventos de lamport.py
    ruta_cond = datos / f"condiciones_{args.equipo}.csv"
    with open(ruta_cond, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["campo", "valor"])
        escritor.writerows(condiciones.recolectar())

    ruta_manifiesto = datos / f"corridas_{args.equipo}_{datetime.now():%Y%m%d-%H%M%S}.csv"
    with open(ruta_manifiesto, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["orden", "etiqueta", "mecanismo", "tipo", "escenario", "tamano_bytes",
                           "repeticion", "nodos", "inicio", "duracion_s", "estado", "detalle"])
        for i, c in enumerate(plan, 1):
            inicio = datetime.now()
            t0 = time.perf_counter()
            estado, detalle = ejecutar(c, args.equipo, carpeta_logs, args.timeout, modulos, datos)
            duracion = time.perf_counter() - t0
            escritor.writerow([i, c.etiqueta, c.mecanismo, c.tipo, c.escenario, c.tamano or "",
                               c.repeticion, " ".join(c.nodos), inicio.isoformat(timespec="seconds"),
                               f"{duracion:.2f}", estado, detalle])
            f.flush()
            print(f"[{i}/{len(plan)}] {c.mecanismo:7} {c.etiqueta:12} {estado} {detalle}")
            if estado != "ok" and not args.seguir:
                sys.exit(f"Se detuvo en la corrida {i}. Revisa {carpeta_logs}.")

    for mec, (ruta, filas) in juntar_latencia(datos).items():
        print(f"{ruta}: {filas} filas de latencia")
    print(f"\nListo. Resumen de corridas en {ruta_manifiesto}")


if __name__ == "__main__":
    main()
