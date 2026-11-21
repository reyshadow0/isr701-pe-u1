"""Pruebas del plan de src/experimento/correr_experimento.py (no lanza procesos).

Desde la raíz del repositorio:  python -m unittest tests.test_correr_experimento -v
"""

import csv
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from src.experimento.correr_experimento import (COLUMNAS_LATENCIA, Corrida, armar_plan,
                                                comandos, juntar_latencia, revisar_archivos)


def valor(cmd, opcion):
    return cmd[cmd.index(opcion) + 1]


class Plan(unittest.TestCase):
    def setUp(self):
        self.plan = armar_plan()

    def test_cantidad_de_corridas(self):
        # latencia: 2 escenarios x 2 tamaños x (1 calentamiento + 5) x 2 mecanismos = 48
        # lamport: 3 configuraciones de nodos x 5 repeticiones x 2 mecanismos = 30
        tipos = Counter(c.tipo for c in self.plan)
        self.assertEqual(tipos, {"latencia": 48, "lamport": 30})

    def test_un_calentamiento_por_mecanismo_tamano_y_escenario(self):
        r0 = [(c.mecanismo, c.tamano, c.escenario) for c in self.plan if c.repeticion == 0]
        self.assertEqual(len(r0), 8)
        self.assertEqual(len(set(r0)), 8)
        self.assertTrue(all(c.etiqueta.endswith("-r0") for c in self.plan if c.repeticion == 0))

    def test_cinco_repeticiones_medidas(self):
        for mec in ("sockets", "grpc"):
            for tam in (100, 10240):
                reps = sorted(c.repeticion for c in self.plan if c.tipo == "latencia"
                              and c.escenario == "secuencial" and c.mecanismo == mec
                              and c.tamano == tam and c.repeticion > 0)
                self.assertEqual(reps, [1, 2, 3, 4, 5])

    def test_mecanismos_intercalados_y_alternados(self):
        bloque = [c for c in self.plan if c.escenario == "secuencial" and c.tamano == 100]
        orden = [c.mecanismo for c in bloque]
        self.assertEqual(orden[:6], ["sockets", "grpc", "grpc", "sockets", "sockets", "grpc"])
        # cada repetición tiene los dos mecanismos seguidos
        for i in range(0, len(bloque), 2):
            self.assertEqual(bloque[i].etiqueta, bloque[i + 1].etiqueta)
            self.assertNotEqual(bloque[i].mecanismo, bloque[i + 1].mecanismo)


class Comandos(unittest.TestCase):
    def test_modulos_por_defecto_son_los_de_la_guia(self):
        plan = armar_plan()
        for mec, modulo in (("sockets", "src.sockets.client"), ("grpc", "src.grpc.client_grpc")):
            c = next(c for c in plan if c.mecanismo == mec)
            _, clientes = comandos(c, "PRUEBA")
            self.assertTrue(all(valor(cmd, "-m") == modulo for cmd in clientes.values()))

    def test_todos_los_nodos_reciben_la_misma_etiqueta(self):
        for c in armar_plan():
            servidores, clientes = comandos(c, "PRUEBA")
            cmds = list(servidores.values()) + list(clientes.values())
            self.assertEqual(len(cmds), len(c.nodos))
            self.assertEqual({valor(cmd, "--corrida") for cmd in cmds}, {c.etiqueta})
            self.assertEqual({valor(cmd, "--equipo") for cmd in cmds}, {"PRUEBA"})

    def test_concurrente_tiene_tres_clientes_contra_un_servidor(self):
        c = next(c for c in armar_plan() if c.escenario == "concurrente")
        servidores, clientes = comandos(c, "PRUEBA")
        self.assertEqual(list(servidores), ["nodo-A"])
        self.assertEqual(sorted(clientes), ["nodo-B", "nodo-C", "nodo-D"])
        self.assertTrue(all(valor(cmd, "--destino") == "nodo-A" for cmd in clientes.values()))

    def test_lamport_con_2_3_y_4_nodos(self):
        n = Counter(len(c.nodos) for c in armar_plan(solo="lamport"))
        self.assertEqual(n, {2: 10, 3: 10, 4: 10})


def escribir_latencia(carpeta, mec, nodo, etiqueta, envios=100):
    ruta = Path(carpeta) / f"latencia_{mec}_{nodo}_PRUEBA_{etiqueta}.csv"
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNAS_LATENCIA)
        for i in range(1, envios + 1):
            w.writerow([mec, "concurrente", 100, f"PRUEBA_{etiqueta}", nodo, i, "0.250", "1.0"])
    return ruta


class Latencia(unittest.TestCase):
    def test_junta_un_archivo_por_mecanismo_sin_cambiar_filas(self):
        with tempfile.TemporaryDirectory() as carpeta:
            for nodo in ("nodo-B", "nodo-C", "nodo-D"):
                escribir_latencia(carpeta, "grpc", nodo, "c100-r1")
            escribir_latencia(carpeta, "sockets", "nodo-B", "s100-r1")
            resultado = juntar_latencia(carpeta)
            with open(Path(carpeta) / "latency_grpc.csv", newline="", encoding="utf-8") as f:
                filas = list(csv.DictReader(f))
        self.assertEqual(resultado["grpc"][1], 300)
        self.assertEqual(resultado["sockets"][1], 100)
        self.assertEqual(Counter(f["nodo"] for f in filas),
                         {"nodo-B": 100, "nodo-C": 100, "nodo-D": 100})

    def test_rechaza_columnas_distintas(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta) / "latencia_grpc_nodo-B_PRUEBA_s100-r1.csv"
            ruta.write_text("mecanismo,rtt_ms\ngrpc,0.2\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                juntar_latencia(carpeta)

    def test_revisa_que_cada_cliente_deje_sus_100_envios(self):
        c = Corrida("c100-r1", "grpc", "latencia", "concurrente",
                    ["nodo-A", "nodo-B", "nodo-C", "nodo-D"], 1, 100)
        with tempfile.TemporaryDirectory() as carpeta:
            for nodo in c.nodos:
                (Path(carpeta) / f"eventos_grpc_{nodo}_PRUEBA_c100-r1.csv").write_text("x", encoding="utf-8")
            escribir_latencia(carpeta, "grpc", "nodo-B", "c100-r1")
            escribir_latencia(carpeta, "grpc", "nodo-C", "c100-r1")
            self.assertEqual(revisar_archivos(c, "PRUEBA", carpeta)[0], "error")   # falta D
            escribir_latencia(carpeta, "grpc", "nodo-D", "c100-r1", envios=99)
            estado, detalle = revisar_archivos(c, "PRUEBA", carpeta)
            self.assertEqual(estado, "error")
            self.assertIn("99", detalle)


if __name__ == "__main__":
    unittest.main()
