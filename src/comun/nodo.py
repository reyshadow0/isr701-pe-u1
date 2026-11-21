"""Lógica común de un nodo, igual para sockets y gRPC.

Aquí está todo lo que no depende del mecanismo: los argumentos, a quién enviar,
la ronda de las dos fichas, cuándo termina cada modo y el CSV de latencia. El
programa de cada mecanismo (src/sockets/client.py y src/grpc/client_grpc.py)
solo tiene que poner el transporte: un servidor que reciba y una función que envíe.

Cómo se usa desde el programa de un mecanismo:

    from src.comun.nodo import NodoComun, leer_argumentos

    args = leer_argumentos()
    comun = NodoComun(args, mecanismo="sockets")

    # 1. Servidor en un hilo. Por cada mensaje que llega:
    #        comun.al_recibir(sender, timestamp, message)
    #    y DESPUÉS responde el acuse. al_recibir no se bloquea.
    # 2. Una función enviar(destino, timestamp, texto) que manda el mensaje a
    #    `destino` (puerto: comun.puerto(destino)) y espera el acuse. Debe usar
    #    UNA conexión o UN canal por destino para todos los envíos, y la primera
    #    vez reintentar con comun.reintentar(conectar) hasta que el otro responda.
    #    Si el acuse no es "ok" o falla la conexión, lanza una excepción.
    # 3. Al final:
    #        sys.exit(comun.correr(enviar))

correr() llama a reloj.enviar() antes de cada envío y le pasa el valor a
enviar() como timestamp, así que el programa del mecanismo nunca toca el reloj.
"""

import argparse
import csv
import queue
import threading
import time
from pathlib import Path

from src.comun.lamport import RelojLamport

COLUMNAS_LATENCIA = ["mecanismo", "escenario", "tamano_bytes", "corrida", "nodo",
                     "envio", "rtt_ms", "hora_real"]
FICHAS = 2
LIMITE_LAMPORT = 30.0       # segundos para terminar la ronda
LIMITE_CONEXION = 10.0      # segundos reintentando conectar
PAUSA_CONEXION = 0.1


def leer_argumentos(argv=None):
    """Lee los argumentos de la interfaz que usa correr_experimento.py."""
    p = argparse.ArgumentParser(description="Nodo del experimento PE-U1.")
    p.add_argument("--nodo", required=True)
    p.add_argument("--puerto", type=int, required=True)
    p.add_argument("--corrida", required=True)
    p.add_argument("--equipo", default=None)
    p.add_argument("--pares", required=True, help="nodo-A=5000,nodo-B=5001,...")
    p.add_argument("--modo", required=True, choices=["servidor", "lamport", "latencia"])
    p.add_argument("--mensajes", type=int, default=20)
    p.add_argument("--destino")
    p.add_argument("--envios", type=int, default=100)
    p.add_argument("--tamano", type=int, default=100)
    p.add_argument("--escenario", default="secuencial")
    p.add_argument("--datos", default="data")
    args = p.parse_args(argv)
    if args.modo == "latencia" and not args.destino:
        p.error("--modo latencia necesita --destino")
    return args


def leer_pares(texto):
    """'nodo-A=5000,nodo-B=5001' -> [('nodo-A', 5000), ('nodo-B', 5001)], en ese orden."""
    pares = []
    for parte in texto.split(","):
        nombre, puerto = parte.split("=")
        pares.append((nombre.strip(), int(puerto)))
    return pares


def texto_ficha(ficha, salto):
    return f"ficha={ficha} salto={salto}"


def leer_ficha(texto):
    """'ficha=1 salto=3' -> (1, 3)."""
    campos = dict(parte.split("=") for parte in texto.split())
    return int(campos["ficha"]), int(campos["salto"])


class Ronda:
    """Plan de los intercambios de Lamport: dos fichas que dan vueltas a la ronda.

    Con los nodos N0..N(n-1) en el orden de --pares, la ficha 1 sale de N0 y la
    ficha 2 de N(n//2). Cada ficha hace mensajes/2 saltos, y el salto k de una
    ficha que salió de Ns va de N(s+k-1) a N(s+k), módulo n.
    """

    def __init__(self, nodos, mensajes=20, fichas=FICHAS):
        if mensajes % fichas:
            raise ValueError(f"{mensajes} mensajes no se reparten en {fichas} fichas")
        self.nodos = list(nodos)
        self.n = len(self.nodos)
        self.saltos = mensajes // fichas
        self.salidas = {f: [0, self.n // 2][f - 1] for f in range(1, fichas + 1)}

    def sucesor(self, nodo):
        return self.nodos[(self.nodos.index(nodo) + 1) % self.n]

    def iniciales(self, nodo):
        """Fichas que arranca este nodo con su salto 1: [(ficha, destino)]."""
        return [(f, self.sucesor(nodo)) for f, s in self.salidas.items()
                if self.nodos[s] == nodo]

    def siguiente(self, nodo, ficha, salto):
        """Al recibir (ficha, salto): (destino, salto + 1) si hay que reenviar, o None."""
        if salto >= self.saltos:
            return None
        return self.sucesor(nodo), salto + 1

    def esperados(self, nodo):
        """(cuántos mensajes envía, cuántos recibe) este nodo en toda la corrida."""
        envia = recibe = 0
        for s in self.salidas.values():
            for k in range(1, self.saltos + 1):
                if self.nodos[(s + k - 1) % self.n] == nodo:
                    envia += 1
                if self.nodos[(s + k) % self.n] == nodo:
                    recibe += 1
        return envia, recibe


class RegistroLatencia:
    """CSV de latencia de UN cliente en UNA corrida. Falla si el archivo ya existe."""

    def __init__(self, mecanismo, nodo, equipo, etiqueta, id_corrida, escenario, tamano,
                 carpeta="data"):
        carpeta = Path(carpeta)
        carpeta.mkdir(parents=True, exist_ok=True)
        self.ruta = carpeta / f"latencia_{mecanismo}_{nodo}_{equipo}_{etiqueta}.csv"
        self._archivo = open(self.ruta, "x", newline="", encoding="utf-8")
        self._escritor = csv.writer(self._archivo)
        self._escritor.writerow(COLUMNAS_LATENCIA)
        self._fijo = [mecanismo, escenario, tamano, id_corrida, nodo]

    def escribir(self, envio, rtt_ms):
        self._escritor.writerow(self._fijo + [envio, f"{rtt_ms:.3f}", f"{time.time():.6f}"])

    def cerrar(self):
        self._archivo.close()


class NodoComun:
    """Estado y comportamiento de un nodo que no dependen del mecanismo."""

    def __init__(self, args, mecanismo):
        self.args = args
        self.mecanismo = mecanismo
        self.nombre = args.nodo
        self.pares = dict(leer_pares(args.pares))
        if self.nombre not in self.pares:
            raise ValueError(f"{self.nombre} no está en --pares")
        self.reloj = RelojLamport(self.nombre, mecanismo, args.corrida, args.equipo, args.datos)
        self.ronda = Ronda(list(self.pares), args.mensajes) if args.modo == "lamport" else None
        self._entrantes = queue.Queue()     # (origen, timestamp, texto) en orden de llegada

    def puerto(self, destino):
        return self.pares[destino]

    def al_recibir(self, origen, timestamp, texto):
        """Lo llama el servidor por cada mensaje, antes de responder el acuse.

        En modo lamport solo encola: el hilo principal registra la recepción y
        reenvía en orden de llegada, después de enviar los saltos 1 que le tocan.
        En los otros modos registra la recepción en el momento.
        """
        if self.args.modo == "lamport":
            self._entrantes.put((origen, int(timestamp), texto))
        else:
            self.reloj.recibir(timestamp, origen)

    @staticmethod
    def reintentar(conectar, limite=LIMITE_CONEXION, pausa=PAUSA_CONEXION):
        """Llama a conectar() hasta que no lance excepción; devuelve lo que devuelva."""
        fin = time.monotonic() + limite
        while True:
            try:
                return conectar()
            except Exception:
                if time.monotonic() >= fin:
                    raise
                time.sleep(pausa)

    def correr(self, enviar):
        """Hace lo que pide --modo y devuelve el código de salida (0 bien, 1 mal)."""
        try:
            if self.args.modo == "servidor":
                while True:                  # correr_experimento.py cierra el proceso
                    time.sleep(1)
            if self.args.modo == "lamport":
                return self._ronda_lamport(enviar)
            return self._latencia(enviar)
        except Exception as error:
            print(f"[{self.nombre}] error: {error}", flush=True)
            return 1
        finally:
            self.reloj.cerrar()

    def _enviar(self, enviar, destino, texto):
        ts = self.reloj.enviar(destino)
        enviar(destino, ts, texto)

    def _ronda_lamport(self, enviar, limite=LIMITE_LAMPORT):
        por_enviar, por_recibir = self.ronda.esperados(self.nombre)
        enviados = recibidos = 0
        for ficha, destino in self.ronda.iniciales(self.nombre):     # antes de recibir nada
            self._enviar(enviar, destino, texto_ficha(ficha, 1))
            enviados += 1
        fin = time.monotonic() + limite
        while enviados < por_enviar or recibidos < por_recibir:
            restante = fin - time.monotonic()
            if restante <= 0:
                print(f"[{self.nombre}] plazo vencido: enviados {enviados}/{por_enviar}, "
                      f"recibidos {recibidos}/{por_recibir}", flush=True)
                return 1
            try:
                origen, ts, texto = self._entrantes.get(timeout=restante)
            except queue.Empty:
                continue
            self.reloj.recibir(ts, origen)
            recibidos += 1
            ficha, salto = leer_ficha(texto)
            sigue = self.ronda.siguiente(self.nombre, ficha, salto)
            if sigue:
                destino, nuevo_salto = sigue
                self._enviar(enviar, destino, texto_ficha(ficha, nuevo_salto))
                enviados += 1
        return 0

    def _latencia(self, enviar):
        a = self.args
        texto = "x" * a.tamano                      # ASCII: un carácter = un byte
        registro = RegistroLatencia(self.mecanismo, self.nombre, self.reloj.equipo, a.corrida,
                                    self.reloj.id_corrida, a.escenario, a.tamano, a.datos)
        try:
            for i in range(1, a.envios + 1):
                ts = self.reloj.enviar(a.destino)  # fuera de la medición
                t0 = time.perf_counter()
                enviar(a.destino, ts, texto)       # hasta recibir el acuse
                registro.escribir(i, (time.perf_counter() - t0) * 1000)
        finally:
            registro.cerrar()
        return 0
