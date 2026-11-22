"""Pruebas de analysis/verificar_lamport.py con CSV escritos por el RelojLamport real.

Desde la raíz del repositorio:  python -m unittest tests.test_verificar_lamport -v
"""

import csv
import tempfile
from collections import deque
import unittest
from pathlib import Path
from unittest import mock

from analysis import verificar_lamport as v
from src.comun.lamport import RelojLamport

EQUIPO = "PRUEBA"
NODOS = ["nodo-A", "nodo-B", "nodo-C", "nodo-D"]


def ronda(carpeta, corrida, n=3, fichas=2, mensajes=20):
    """Simula los intercambios tal como los pide la interfaz de correr_experimento.

    La ficha 1 sale de N0 y la ficha 2 de N(n//2). Los que arrancan envían su
    salto 1 antes de recibir nada; después cada mensaje se procesa en orden de
    llegada y, si no es el último salto de su ficha, se reenvía al sucesor.
    Con fichas=1 es la ronda de una sola ficha que describe H4.
    """
    nodos = NODOS[:n]
    relojes = {nd: RelojLamport(nd, "sockets", corrida, EQUIPO, carpeta) for nd in nodos}
    saltos = mensajes // fichas
    en_vuelo = deque()
    for ficha, salida in enumerate([0, n // 2][:fichas], start=1):
        origen, destino = nodos[salida], nodos[(salida + 1) % n]
        en_vuelo.append((ficha, 1, origen, destino, relojes[origen].enviar(destino)))
    while en_vuelo:
        ficha, salto, origen, destino, ts = en_vuelo.popleft()
        relojes[destino].recibir(ts, origen)
        if salto < saltos:
            siguiente = nodos[(nodos.index(destino) + 1) % n]
            en_vuelo.append((ficha, salto + 1, destino, siguiente,
                             relojes[destino].enviar(siguiente)))
    for r in relojes.values():
        r.cerrar()


def editar(ruta, cambiar):
    """Reescribe un CSV aplicando cambiar(filas) -> filas. Solo para simular errores."""
    with open(ruta, newline="", encoding="utf-8") as f:
        filas = list(csv.DictReader(f))
    filas = cambiar(filas)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=v.COLUMNAS)
        w.writeheader()
        w.writerows(filas)


def verificar(carpeta):
    eventos, _ = v.cargar(str(Path(carpeta) / "eventos_*.csv"))
    pares, sin_recepcion = v.emparejar(eventos)
    return eventos, pares, sin_recepcion


class DatosCorrectos(unittest.TestCase):
    def test_veinte_mensajes_todos_cumplen(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            _, pares, sin_recepcion = verificar(carpeta)
        self.assertEqual(len(pares), 20)
        self.assertTrue(pares["cumple"].all())
        self.assertTrue(pares["destino_ok"].all())
        self.assertEqual(len(sin_recepcion), 0)

    def test_dos_corridas_no_se_mezclan(self):
        # Las dos corridas tienen los mismos relojes; solo la columna corrida las separa
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            ronda(carpeta, "n3-r2")
            _, pares, sin_recepcion = verificar(carpeta)
        self.assertEqual(len(pares), 40)
        self.assertTrue(pares["cumple"].all())
        self.assertEqual(len(sin_recepcion), 0)

    def test_los_dos_primeros_envios_son_concurrentes(self):
        # nodo-A y nodo-B arrancan las dos fichas con reloj 1 sin haber recibido nada
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            eventos, _, _ = verificar(carpeta)
        empates = v.buscar_empates(eventos)
        primeros = empates[empates["reloj"] == 1]
        self.assertEqual(len(primeros), 1)
        fila = primeros.iloc[0]
        self.assertEqual((fila["nodo_a"], fila["nodo_b"]), ("nodo-A", "nodo-B"))
        self.assertEqual((fila["evento_a"], fila["evento_b"]), ("envio", "envio"))

    def test_main_sale_con_0(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            argv = ["x", "--eventos", f"{carpeta}/eventos_*.csv", "--salida", carpeta]
            with mock.patch("sys.argv", argv), self.assertRaises(SystemExit) as fin, \
                    mock.patch("builtins.print"):
                v.main()
        self.assertEqual(fin.exception.code, 0)


class Convergencia(unittest.TestCase):
    def convergencia(self, n, fichas):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, f"n{n}-r1", n=n, fichas=fichas)
            eventos, _, _ = verificar(carpeta)
        return v.convergencia(eventos).iloc[0]

    def test_una_ficha_da_2n_menos_2(self):
        # Es la cuenta de H4: con una sola ficha, 2, 4 y 6 intercambios
        for n in (2, 3, 4):
            fila = self.convergencia(n, fichas=1)
            self.assertTrue(fila["converge"])
            self.assertEqual(fila["intercambios"], 2 * n - 2, f"{n} nodos")

    def test_dos_fichas(self):
        # Con dos fichas se cuentan los mensajes de las dos, así que hacen falta más
        # intercambios (2, 6 y 8), pero se llega con un reloj menor que con una ficha
        # (2, 6 y 8 frente a 4, 8 y 12): la información da menos saltos seguidos.
        # Cada nodo recibe de un solo antecesor por una sola conexión, así que el
        # orden de los eventos y estos números son siempre los mismos.
        esperado = {2: (2, 2), 3: (6, 6), 4: (8, 8)}
        for n, (intercambios, reloj) in esperado.items():
            fila = self.convergencia(n, fichas=2)
            una_ficha = self.convergencia(n, fichas=1)
            self.assertTrue(fila["converge"])
            self.assertEqual((fila["intercambios"], fila["reloj"]), (intercambios, reloj), f"{n} nodos")
            self.assertLess(fila["reloj"], una_ficha["reloj"])
            self.assertGreaterEqual(fila["segundos"], 0)

    def test_corridas_de_latencia_no_cuentan(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "s100-r1")
            eventos, _, _ = verificar(carpeta)
        self.assertTrue(v.convergencia(eventos).empty)


class DatosConErrores(unittest.TestCase):
    def test_detecta_recepcion_con_reloj_menor(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-B_*.csv"))

            def bajar_reloj(filas):
                for f in filas:
                    if f["evento"] == "recepcion":
                        f["reloj"] = f["reloj_emisor"]      # igual al del emisor: no cumple
                        break
                return filas

            editar(ruta, bajar_reloj)
            _, pares, _ = verificar(carpeta)
        self.assertEqual(int((~pares["cumple"]).sum()), 1)

    def test_detecta_mensaje_perdido(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-C_*.csv"))
            eventos, _, _ = verificar(carpeta)
            hacia_c = int(((eventos["evento"] == "envio") & (eventos["destino"] == "nodo-C")).sum())
            # Se borran las recepciones de nodo-C: los mensajes hacia C quedan sin pareja
            editar(ruta, lambda filas: [f for f in filas if f["evento"] != "recepcion"])
            _, pares, sin_recepcion = verificar(carpeta)
        self.assertGreater(hacia_c, 0)
        self.assertTrue(pares["cumple"].all())
        self.assertEqual(len(sin_recepcion), hacia_c)
        self.assertTrue((sin_recepcion["destino"] == "nodo-C").all())

    def test_detecta_recepcion_sin_envio(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-A_*.csv"))
            editar(ruta, lambda filas: [f for f in filas if f["evento"] != "envio"])
            _, pares, _ = verificar(carpeta)
        self.assertGreater(int((~pares["con_envio"]).sum()), 0)

    def test_main_sale_con_1(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ronda(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-C_*.csv"))
            editar(ruta, lambda filas: filas[1:])
            argv = ["x", "--eventos", f"{carpeta}/eventos_*.csv", "--salida", carpeta]
            with mock.patch("sys.argv", argv), self.assertRaises(SystemExit) as fin, \
                    mock.patch("builtins.print"):
                v.main()
        self.assertEqual(fin.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
