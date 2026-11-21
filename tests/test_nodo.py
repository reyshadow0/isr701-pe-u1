"""Pruebas de src/comun/nodo.py con un transporte en memoria (sin red).

Desde la raíz del repositorio:  python -m unittest tests.test_nodo -v
"""

import csv
import tempfile
import threading
import unittest
from pathlib import Path

from analysis import verificar_lamport as v
from src.comun.nodo import (COLUMNAS_LATENCIA, NodoComun, Ronda, leer_argumentos,
                            leer_ficha, texto_ficha)

EQUIPO = "PRUEBA"
NODOS = ["nodo-A", "nodo-B", "nodo-C", "nodo-D"]


def pares(n):
    return ",".join(f"{nodo}={5000 + i}" for i, nodo in enumerate(NODOS[:n]))


def crear(nodo, n, modo, carpeta, corrida, *extra):
    args = leer_argumentos(["--nodo", nodo, "--puerto", "0", "--corrida", corrida,
                            "--equipo", EQUIPO, "--pares", pares(n), "--modo", modo,
                            "--datos", carpeta, *extra])
    return NodoComun(args, "sockets")


def transporte(nodos, origen):
    """enviar() en memoria: entrega directo a al_recibir del destino (el 'acuse' es volver)."""
    def enviar(destino, timestamp, texto):
        nodos[destino].al_recibir(origen, timestamp, texto)
    return enviar


def correr_ronda(carpeta, n, corrida):
    nodos = {nd: crear(nd, n, "lamport", carpeta, corrida) for nd in NODOS[:n]}
    codigos = {}
    hilos = [threading.Thread(target=lambda nd=nd: codigos.__setitem__(
        nd, nodos[nd].correr(transporte(nodos, nd)))) for nd in nodos]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join(timeout=20)
    return codigos


class PlanDeLaRonda(unittest.TestCase):
    def test_veinte_mensajes_en_total(self):
        for n in (2, 3, 4):
            ronda = Ronda(NODOS[:n], 20)
            envios = sum(ronda.esperados(nd)[0] for nd in NODOS[:n])
            recepciones = sum(ronda.esperados(nd)[1] for nd in NODOS[:n])
            self.assertEqual((envios, recepciones), (20, 20), f"{n} nodos")

    def test_quien_arranca(self):
        self.assertEqual(Ronda(NODOS[:2]).iniciales("nodo-B"), [(2, "nodo-A")])
        self.assertEqual(Ronda(NODOS[:3]).iniciales("nodo-A"), [(1, "nodo-B")])
        self.assertEqual(Ronda(NODOS[:3]).iniciales("nodo-B"), [(2, "nodo-C")])
        self.assertEqual(Ronda(NODOS[:3]).iniciales("nodo-C"), [])
        self.assertEqual(Ronda(NODOS[:4]).iniciales("nodo-C"), [(2, "nodo-D")])

    def test_el_ultimo_salto_no_se_reenvia(self):
        ronda = Ronda(NODOS[:3], 20)
        self.assertEqual(ronda.siguiente("nodo-C", 1, 9), ("nodo-A", 10))
        self.assertIsNone(ronda.siguiente("nodo-A", 1, 10))

    def test_texto_de_la_ficha(self):
        self.assertEqual(leer_ficha(texto_ficha(2, 7)), (2, 7))


class RondaCompleta(unittest.TestCase):
    def test_todos_terminan_y_los_csv_pasan_la_verificacion(self):
        esperado = {2: 2, 3: 6, 4: 8}          # intercambios hasta la convergencia
        for n, intercambios in esperado.items():
            with tempfile.TemporaryDirectory() as carpeta:
                codigos = correr_ronda(carpeta, n, f"n{n}-r1")
                eventos, _ = v.cargar(f"{carpeta}/eventos_*.csv")
                pares_, sin_recepcion = v.emparejar(eventos)
                conv = v.convergencia(eventos).iloc[0]
                empates = v.buscar_empates(eventos)
            self.assertEqual(codigos, {nd: 0 for nd in NODOS[:n]}, f"{n} nodos")
            self.assertEqual(len(pares_), 20)
            self.assertTrue(pares_["cumple"].all())
            self.assertEqual(len(sin_recepcion), 0)
            self.assertEqual(conv["intercambios"], intercambios, f"{n} nodos")
            # los dos primeros envíos salen con reloj 1 y son concurrentes
            primeros = empates[empates["reloj"] == 1]
            self.assertEqual(len(primeros), 1)
            self.assertEqual((primeros.iloc[0]["evento_a"], primeros.iloc[0]["evento_b"]),
                             ("envio", "envio"))

    def test_sin_respuesta_vence_el_plazo(self):
        with tempfile.TemporaryDirectory() as carpeta:
            a = crear("nodo-A", 3, "lamport", carpeta, "n3-r1")
            codigo = a._ronda_lamport(lambda destino, ts, texto: None, limite=0.3)
            a.reloj.cerrar()
        self.assertEqual(codigo, 1)


class Latencia(unittest.TestCase):
    def test_cliente_escribe_sus_100_envios(self):
        with tempfile.TemporaryDirectory() as carpeta:
            servidor = crear("nodo-A", 2, "servidor", carpeta, "s100-r1")
            cliente = crear("nodo-B", 2, "latencia", carpeta, "s100-r1",
                            "--destino", "nodo-A", "--envios", "100", "--tamano", "100")
            tamanos = set()

            def enviar(destino, ts, texto):
                tamanos.add(len(texto.encode("utf-8")))
                servidor.al_recibir("nodo-B", ts, texto)

            codigo = cliente.correr(enviar)
            servidor.reloj.cerrar()
            ruta = Path(carpeta) / "latencia_sockets_nodo-B_PRUEBA_s100-r1.csv"
            with open(ruta, newline="", encoding="utf-8") as f:
                lector = csv.reader(f)
                encabezado = next(lector)
                filas = list(lector)
            eventos, _ = v.cargar(f"{carpeta}/eventos_*.csv")
            pares_, sin_recepcion = v.emparejar(eventos)
        self.assertEqual(codigo, 0)
        self.assertEqual(tamanos, {100})
        self.assertEqual(encabezado, COLUMNAS_LATENCIA)
        self.assertEqual([int(f[5]) for f in filas], list(range(1, 101)))
        self.assertEqual({f[3] for f in filas}, {"PRUEBA_s100-r1"})
        self.assertEqual(len(pares_), 100)
        self.assertTrue(pares_["cumple"].all())
        self.assertEqual(len(sin_recepcion), 0)


class Reintentar(unittest.TestCase):
    def test_reintenta_hasta_que_responde(self):
        intentos = []

        def conectar():
            intentos.append(1)
            if len(intentos) < 3:
                raise ConnectionRefusedError
            return "conectado"

        self.assertEqual(NodoComun.reintentar(conectar, limite=2, pausa=0.01), "conectado")
        self.assertEqual(len(intentos), 3)

    def test_se_rinde_al_vencer_el_plazo(self):
        def conectar():
            raise ConnectionRefusedError

        with self.assertRaises(ConnectionRefusedError):
            NodoComun.reintentar(conectar, limite=0.2, pausa=0.05)


if __name__ == "__main__":
    unittest.main()
