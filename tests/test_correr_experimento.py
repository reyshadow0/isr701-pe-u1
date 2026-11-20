"""Pruebas del plan de src/experimento/correr_experimento.py (no lanza procesos).

Desde la raíz del repositorio:  python -m unittest tests.test_correr_experimento -v
"""

import unittest
from collections import Counter

from src.experimento.correr_experimento import armar_plan, comandos


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


if __name__ == "__main__":
    unittest.main()
