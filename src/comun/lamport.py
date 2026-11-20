"""Reloj lógico de Lamport y registro de eventos en CSV.

Cada nodo es un solo proceso y crea un solo RelojLamport al arrancar. El servidor
corre en un hilo y el cliente en el mismo programa, y los dos usan ese reloj:

    reloj = RelojLamport("nodo-A", "sockets", corrida="n3-r2")
    ts = reloj.enviar("nodo-B")                  # ts va en el timestamp del mensaje
    reloj.recibir(mensaje.timestamp, "nodo-B")

`corrida` es la etiqueta que el experimento le da a la corrida, y tiene que ser la
misma en todos los nodos de esa corrida (por ejemplo "calentamiento" o "n3-r2").
El identificador que se guarda le antepone el equipo (por defecto, el nombre
de la máquina): <equipo>_n3-r2.

Cada corrida de cada nodo va a su propio archivo nuevo, por ejemplo
data/eventos_sockets_nodo-A_<equipo>_n3-r2.csv. Si el archivo ya existe, el
reloj no arranca: así no se mezclan corridas ni escriben dos relojes del mismo
nodo en el mismo archivo.

Los nombres (nodo, origen, destino, mecanismo, equipo y corrida) solo aceptan
letras sin tilde, números, punto y guion. Un nombre como "nodo_C" lanza ValueError.
"""

import csv
import re
import socket
import threading
import time
from pathlib import Path

COLUMNAS = [
    "corrida",
    "mecanismo",
    "nodo",
    "evento",
    "reloj",
    "reloj_emisor",
    "origen",
    "destino",
    "hora_real",
]


_NOMBRE_VALIDO = re.compile(r"[A-Za-z0-9.-]+")


def _validar(campo: str, valor) -> str:
    """Acepta solo letras sin tilde, números, punto y guion, y si no, lanza ValueError.

    No corrige el nombre: lo que se guarda en nodo, origen y destino tiene que ser
    exactamente lo que se pasó, para que Dayver pueda emparejar las filas. El guion
    bajo queda fuera porque separa el equipo de la etiqueta en el id de corrida.
    """
    valor = str(valor)
    if not _NOMBRE_VALIDO.fullmatch(valor):
        raise ValueError(
            f"{campo} inválido: {valor!r}. Solo se permiten letras sin tilde, "
            f"números, punto y guion."
        )
    return valor


class RegistroEventos:
    """CSV de eventos de un nodo, un mecanismo y una corrida.

    No tiene lock propio: quien llama a escribir() ya debe tener tomado el lock
    del reloj, para que el orden de las filas sea el mismo que el del reloj.
    """

    def __init__(self, nodo: str, mecanismo: str, id_corrida: str, carpeta: str | Path = "data"):
        carpeta = Path(carpeta)
        carpeta.mkdir(parents=True, exist_ok=True)
        self.ruta = carpeta / f"eventos_{mecanismo}_{nodo}_{id_corrida}.csv"
        try:
            # "x" crea el archivo y falla si ya existe
            self._archivo = open(self.ruta, "x", newline="", encoding="utf-8")
        except FileExistsError:
            raise FileExistsError(
                f"{self.ruta} ya existe. Usa otra etiqueta de corrida, o revisa que "
                f"{nodo} no tenga otro proceso con su propio reloj."
            ) from None
        self._escritor = csv.writer(self._archivo)
        self._escritor.writerow(COLUMNAS)
        self._archivo.flush()

    def escribir(self, fila: list) -> None:
        self._escritor.writerow(fila)
        # flush en cada fila para no perder eventos si cortan el nodo con Ctrl+C.
        # Esto ocurre dentro del lock y en el camino en que el servidor responde.
        self._archivo.flush()

    def cerrar(self) -> None:
        self._archivo.close()


class RelojLamport:
    """Reloj de Lamport de un nodo. Todo cambio del valor ocurre dentro del lock."""

    def __init__(
        self,
        nodo: str,
        mecanismo: str,
        corrida: str,
        equipo: str | None = None,
        carpeta: str | Path = "data",
    ):
        self.nodo = _validar("nodo", nodo)
        self.mecanismo = _validar("mecanismo", mecanismo)
        # Por defecto el nombre de la máquina. Si tiene caracteres no válidos, pasa equipo a mano.
        self.equipo = _validar("equipo", equipo or socket.gethostname())
        self.id_corrida = f"{self.equipo}_{_validar('corrida', corrida)}"
        self._valor = 0
        self._lock = threading.Lock()
        self._registro = RegistroEventos(self.nodo, self.mecanismo, self.id_corrida, carpeta)

    @property
    def ruta(self) -> Path:
        return self._registro.ruta

    @property
    def valor(self) -> int:
        with self._lock:
            return self._valor

    def enviar(self, destino: str) -> int:
        """Suma 1 y devuelve el valor que va en el timestamp del mensaje."""
        destino = _validar("destino", destino)
        with self._lock:
            self._valor += 1
            self._registrar("envio", "", self.nodo, destino)
            return self._valor

    def recibir(self, timestamp: int, origen: str) -> int:
        """Deja el reloj en max(local, recibido) + 1 y devuelve el valor nuevo."""
        timestamp = int(timestamp)
        origen = _validar("origen", origen)
        with self._lock:
            self._valor = max(self._valor, timestamp) + 1
            self._registrar("recepcion", timestamp, origen, self.nodo)
            return self._valor

    def cerrar(self) -> None:
        with self._lock:
            self._registro.cerrar()

    def _registrar(self, evento: str, reloj_emisor, origen: str, destino: str) -> None:
        # Se llama con self._lock tomado. reloj es el valor después del evento.
        self._registro.escribir([
            self.id_corrida,
            self.mecanismo,
            self.nodo,
            evento,
            self._valor,
            reloj_emisor,
            origen,
            destino,
            f"{time.time():.6f}",
        ])