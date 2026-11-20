"""Corre el experimento completo de la PE-U1 con sockets y gRPC.

Uso (desde la raíz del repo):
    python -m src.experimento.correr_experimento --dry-run    # muestra el plan, no corre nada
    python -m src.experimento.correr_experimento              # corrida oficial
    python -m src.experimento.correr_experimento --solo latencia

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

Interfaz que debe cumplir el programa de cada mecanismo (src/sockets/nodo.py y
src/grpc/nodo.py). Un nodo es UN proceso con un solo RelojLamport:
    python -m src.<mecanismo>.nodo --nodo nodo-A --puerto 5000 --corrida n3-r1
        --equipo <equipo> --pares nodo-A=5000,nodo-B=5001,nodo-C=5002
        --modo servidor                     solo atiende, hasta que este script lo cierre
        --modo lamport --mensajes 20        hace su parte de los 20 intercambios y termina
        --modo latencia --destino nodo-A --envios 100 --tamano 100 --escenario secuencial
                                            mide el RTT de cada envío y termina
El programa sale con código 0 si todo salió bien y con otro código si no.
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
MODULOS = {"sockets": "src.sockets.nodo", "grpc": "src.grpc.nodo"}
PUERTOS = {"nodo-A": 5000, "nodo-B": 5001, "nodo-C": 5002, "nodo-D": 5003}
TAMANOS = (100, 10240)              # bytes del campo message: ~100 B y 10 KB
REPETICIONES = 5                    # r1..r5; r0 es el calentamiento
ENVIOS = 100
MENSAJES_LAMPORT = 20
NODOS_LAMPORT = (2, 3, 4)
SERVIDOR = "nodo-A"
CLIENTES = {"secuencial": ["nodo-B"], "concurrente": ["nodo-B", "nodo-C", "nodo-D"]}
PREFIJO = {"secuencial": "s", "concurrente": "c"}


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


def ejecutar(corrida, equipo, carpeta_logs, limite, modulos=MODULOS):
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
        return "ok", ""
    finally:
        cerrar(lanzados)
        for log in abiertos:
            log.close()


def archivos_existentes(plan, equipo, carpeta):
    """CSV de eventos que ya existen para las etiquetas del plan (lamport.py no los pisa)."""
    existentes = []
    for c in plan:
        existentes += glob_eventos(carpeta, c.mecanismo, equipo, c.etiqueta)
    return existentes


def glob_eventos(carpeta, mecanismo, equipo, etiqueta):
    return sorted(Path(carpeta).glob(f"eventos_{mecanismo}_*_{equipo}_{etiqueta}.csv"))


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
    args = parser.parse_args()

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
    carpeta_logs = datos / "logs"
    carpeta_logs.mkdir(parents=True, exist_ok=True)

    # Las condiciones de la máquina quedan junto a los datos de la corrida
    ruta_cond = datos / f"condiciones_{args.equipo.lower()}.csv"
    with open(ruta_cond, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["campo", "valor"])
        escritor.writerows(condiciones.recolectar())

    ruta_manifiesto = datos / f"corridas_{args.equipo.lower()}_{datetime.now():%Y%m%d-%H%M%S}.csv"
    with open(ruta_manifiesto, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["orden", "etiqueta", "mecanismo", "tipo", "escenario", "tamano_bytes",
                           "repeticion", "nodos", "inicio", "duracion_s", "estado", "detalle"])
        for i, c in enumerate(plan, 1):
            inicio = datetime.now()
            t0 = time.perf_counter()
            estado, detalle = ejecutar(c, args.equipo, carpeta_logs, args.timeout, modulos)
            duracion = time.perf_counter() - t0
            escritor.writerow([i, c.etiqueta, c.mecanismo, c.tipo, c.escenario, c.tamano or "",
                               c.repeticion, " ".join(c.nodos), inicio.isoformat(timespec="seconds"),
                               f"{duracion:.2f}", estado, detalle])
            f.flush()
            print(f"[{i}/{len(plan)}] {c.mecanismo:7} {c.etiqueta:12} {estado} {detalle}")
            if estado != "ok" and not args.seguir:
                sys.exit(f"Se detuvo en la corrida {i}. Revisa {carpeta_logs}.")

    print(f"\nListo. Resumen de corridas en {ruta_manifiesto}")


if __name__ == "__main__":
    main()
