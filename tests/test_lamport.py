"""Pruebas de src/comun/lamport.py.

Desde la raíz del repositorio:  python -m unittest tests.test_lamport -v
"""

import csv
import tempfile
import threading
import unittest
from pathlib import Path

from src.comun.lamport import RelojLamport

EQUIPO = "PRUEBA"  # fijo, para no depender del nombre de la máquina


def leer(ruta: Path) -> list[dict]:
    with open(ruta, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class ReglasDelReloj(unittest.TestCase):
    def test_empieza_en_0(self):
        with tempfile.TemporaryDirectory() as carpeta:
            r = RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)
            valor = r.valor
            r.cerrar()
            self.assertEqual(valor, 0)

    def test_envio_suma_1_y_devuelve_el_valor(self):
        with tempfile.TemporaryDirectory() as carpeta:
            r = RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)
            devueltos = [r.enviar("nodo-B"), r.enviar("nodo-C")]
            r.cerrar()
            filas = leer(r.ruta)
            self.assertEqual(devueltos, [1, 2])
            self.assertEqual([f["reloj"] for f in filas], ["1", "2"])
            self.assertEqual(filas[0]["reloj_emisor"], "")
            self.assertEqual((filas[0]["origen"], filas[0]["destino"]), ("nodo-A", "nodo-B"))

    def test_recepcion_con_timestamp_mayor(self):
        # Caso del servidor que recibe 50: queda en 51, y el siguiente envío sale con 52
        with tempfile.TemporaryDirectory() as carpeta:
            r = RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)
            recibido = r.recibir(50, "nodo-B")
            enviado = r.enviar("nodo-C")
            r.cerrar()
            fila = leer(r.ruta)[0]
            self.assertEqual((recibido, enviado), (51, 52))
            self.assertEqual((fila["reloj"], fila["reloj_emisor"]), ("51", "50"))
            self.assertEqual((fila["origen"], fila["destino"]), ("nodo-B", "nodo-A"))

    def test_recepcion_con_timestamp_menor_o_igual(self):
        with tempfile.TemporaryDirectory() as carpeta:
            r = RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)
            for _ in range(5):
                r.enviar("nodo-B")  # local en 5
            menor = r.recibir(2, "nodo-B")  # max(5, 2) + 1
            igual = r.recibir(6, "nodo-C")  # max(6, 6) + 1
            r.cerrar()
            self.assertEqual((menor, igual), (6, 7))


class Hilos(unittest.TestCase):
    def test_orden_del_csv_coincide_con_el_reloj(self):
        with tempfile.TemporaryDirectory() as carpeta:
            r = RelojLamport("nodo-B", "grpc", "r1", EQUIPO, carpeta)

            def trabajo():
                for k in range(500):
                    if k % 2:
                        r.enviar("nodo-C")
                    else:
                        r.recibir(k, "nodo-A")

            hilos = [threading.Thread(target=trabajo) for _ in range(8)]
            for h in hilos:
                h.start()
            for h in hilos:
                h.join()
            final = r.valor
            r.cerrar()

            relojes = [int(f["reloj"]) for f in leer(r.ruta)]
            self.assertEqual(len(relojes), 4000)
            self.assertEqual(relojes[-1], final)
            self.assertTrue(all(a < b for a, b in zip(relojes, relojes[1:])))


class Corridas(unittest.TestCase):
    def test_misma_corrida_lanza_file_exists_error(self):
        with tempfile.TemporaryDirectory() as carpeta:
            r = RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)
            r.cerrar()
            with self.assertRaises(FileExistsError):
                RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)

    def test_otra_corrida_usa_otro_archivo(self):
        with tempfile.TemporaryDirectory() as carpeta:
            r1 = RelojLamport("nodo-A", "sockets", "r1", EQUIPO, carpeta)
            r1.enviar("nodo-B")
            r1.cerrar()
            r2 = RelojLamport("nodo-A", "sockets", "r2", EQUIPO, carpeta)
            r2.enviar("nodo-B")
            r2.cerrar()

            self.assertNotEqual(r1.ruta, r2.ruta)
            self.assertEqual(r2.ruta.name, "eventos_sockets_nodo-A_PRUEBA_r2.csv")
            for r, corrida in ((r1, "PRUEBA_r1"), (r2, "PRUEBA_r2")):
                filas = leer(r.ruta)
                self.assertEqual(len(filas), 1)
                self.assertEqual((filas[0]["corrida"], filas[0]["reloj"]), (corrida, "1"))


class Emparejamiento(unittest.TestCase):
    """Cada recepción encuentra su envío por (corrida, origen, reloj_emisor)."""

    def correr(self, carpeta, corrida):
        # Ejemplo de clase: m1 A->B, m2 C->B, m4 A->C, m3 B->C
        a, b, c = (RelojLamport(n, "sockets", corrida, EQUIPO, carpeta)
                   for n in ("nodo-A", "nodo-B", "nodo-C"))
        m1 = a.enviar("nodo-B")
        m2 = c.enviar("nodo-B")
        b.recibir(m1, "nodo-A")
        b.recibir(m2, "nodo-C")
        m4 = a.enviar("nodo-C")
        c.recibir(m4, "nodo-A")
        m3 = b.enviar("nodo-C")
        c.recibir(m3, "nodo-B")
        for r in (a, b, c):
            r.cerrar()

    def test_cada_recepcion_tiene_su_envio(self):
        with tempfile.TemporaryDirectory() as carpeta:
            # Dos corridas iguales: los pares (emisor, reloj) se repiten entre ellas
            self.correr(carpeta, "r1")
            self.correr(carpeta, "r2")
            filas = [f for ruta in sorted(Path(carpeta).glob("eventos_*.csv")) for f in leer(ruta)]

        envios = {(f["corrida"], f["nodo"], f["reloj"]): f for f in filas if f["evento"] == "envio"}
        recepciones = [f for f in filas if f["evento"] == "recepcion"]
        self.assertEqual(len(envios), 8)
        self.assertEqual(len(recepciones), 8)

        pares = []
        for f in recepciones:
            envio = envios[(f["corrida"], f["origen"], f["reloj_emisor"])]
            self.assertEqual(envio["destino"], f["nodo"])
            self.assertGreater(int(f["reloj"]), int(envio["reloj"]))
            pares.append((f["corrida"], int(envio["reloj"]), int(f["reloj"])))

        # m1 1->2, m2 1->3, m4 2->3, m3 4->5, en cada corrida
        for corrida in ("PRUEBA_r1", "PRUEBA_r2"):
            self.assertEqual(sorted(p[1:] for p in pares if p[0] == corrida),
                             [(1, 2), (1, 3), (2, 3), (4, 5)])


class Nombres(unittest.TestCase):
    def test_rechaza_nombres_con_caracteres_no_validos(self):
        with tempfile.TemporaryDirectory() as carpeta:
            with self.assertRaises(ValueError):
                RelojLamport("nodo_C", "sockets", "r1", EQUIPO, carpeta)
            with self.assertRaises(ValueError):
                RelojLamport("nodo-C", "sockets", "r1", "Rey Shadow", carpeta)

            r = RelojLamport("nodo-C", "sockets", "r1", EQUIPO, carpeta)
            with self.assertRaises(ValueError):
                r.recibir(3, "nodo_A")
            with self.assertRaises(ValueError):
                r.enviar("nodo_B")
            valor = r.valor
            r.cerrar()
            # un nombre rechazado no mueve el reloj ni escribe filas
            self.assertEqual(valor, 0)
            self.assertEqual(leer(r.ruta), [])


if __name__ == "__main__":
    unittest.main()