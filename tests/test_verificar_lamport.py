"""Pruebas de analysis/verificar_lamport.py con CSV escritos por el RelojLamport real.

Desde la raíz del repositorio:  python -m unittest tests.test_verificar_lamport -v
"""

import csv
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from analysis import verificar_lamport as v
from src.comun.lamport import RelojLamport

EQUIPO = "PRUEBA"
NODOS = ["nodo-A", "nodo-B", "nodo-C"]


def anillo_por_rondas(carpeta, corrida, mensajes=20):
    """20 mensajes en anillo: en cada ronda cada nodo envía a su sucesor y luego recibe."""
    relojes = {n: RelojLamport(n, "sockets", corrida, EQUIPO, carpeta) for n in NODOS}
    enviados = 0
    while enviados < mensajes:
        en_vuelo = []
        for i, n in enumerate(NODOS):
            if enviados == mensajes:
                break
            destino = NODOS[(i + 1) % len(NODOS)]
            en_vuelo.append((n, destino, relojes[n].enviar(destino)))
            enviados += 1
        for origen, destino, ts in en_vuelo:
            relojes[destino].recibir(ts, origen)
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
            anillo_por_rondas(carpeta, "n3-r1")
            _, pares, sin_recepcion = verificar(carpeta)
        self.assertEqual(len(pares), 20)
        self.assertTrue(pares["cumple"].all())
        self.assertTrue(pares["destino_ok"].all())
        self.assertEqual(len(sin_recepcion), 0)

    def test_dos_corridas_no_se_mezclan(self):
        # Las dos corridas tienen los mismos relojes; solo la columna corrida las separa
        with tempfile.TemporaryDirectory() as carpeta:
            anillo_por_rondas(carpeta, "n3-r1")
            anillo_por_rondas(carpeta, "n3-r2")
            _, pares, sin_recepcion = verificar(carpeta)
        self.assertEqual(len(pares), 40)
        self.assertTrue(pares["cumple"].all())
        self.assertEqual(len(sin_recepcion), 0)

    def test_encuentra_empates_entre_nodos(self):
        # En la ronda 1 los tres nodos envían con reloj 1 sin haber recibido nada
        with tempfile.TemporaryDirectory() as carpeta:
            anillo_por_rondas(carpeta, "n3-r1")
            eventos, _, _ = verificar(carpeta)
        empates = v.buscar_empates(eventos)
        ronda1 = empates[(empates["reloj"] == 1)]
        self.assertEqual(len(ronda1), 3)   # A-B, A-C, B-C
        self.assertTrue((ronda1["evento_a"] == "envio").all())

    def test_main_sale_con_0(self):
        with tempfile.TemporaryDirectory() as carpeta:
            anillo_por_rondas(carpeta, "n3-r1")
            argv = ["x", "--eventos", f"{carpeta}/eventos_*.csv", "--salida", carpeta]
            with mock.patch("sys.argv", argv), self.assertRaises(SystemExit) as fin, \
                    mock.patch("builtins.print"):
                v.main()
        self.assertEqual(fin.exception.code, 0)


class DatosConErrores(unittest.TestCase):
    def test_detecta_recepcion_con_reloj_menor(self):
        with tempfile.TemporaryDirectory() as carpeta:
            anillo_por_rondas(carpeta, "n3-r1")
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
            anillo_por_rondas(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-C_*.csv"))
            # Se borran las recepciones de nodo-C: los mensajes de B a C quedan sin pareja
            editar(ruta, lambda filas: [f for f in filas if f["evento"] != "recepcion"])
            _, pares, sin_recepcion = verificar(carpeta)
        self.assertTrue(pares["cumple"].all())
        # Con 20 mensajes hay 7 rondas y nodo-B envía en las 7 (la última es A->B y B->C)
        self.assertEqual(len(sin_recepcion), 7)
        self.assertTrue((sin_recepcion["destino"] == "nodo-C").all())

    def test_detecta_recepcion_sin_envio(self):
        with tempfile.TemporaryDirectory() as carpeta:
            anillo_por_rondas(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-A_*.csv"))
            editar(ruta, lambda filas: [f for f in filas if f["evento"] != "envio"])
            _, pares, _ = verificar(carpeta)
        self.assertGreater(int((~pares["con_envio"]).sum()), 0)

    def test_main_sale_con_1(self):
        with tempfile.TemporaryDirectory() as carpeta:
            anillo_por_rondas(carpeta, "n3-r1")
            ruta = next(Path(carpeta).glob("eventos_sockets_nodo-C_*.csv"))
            editar(ruta, lambda filas: filas[1:])
            argv = ["x", "--eventos", f"{carpeta}/eventos_*.csv", "--salida", carpeta]
            with mock.patch("sys.argv", argv), self.assertRaises(SystemExit) as fin, \
                    mock.patch("builtins.print"):
                v.main()
        self.assertEqual(fin.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
