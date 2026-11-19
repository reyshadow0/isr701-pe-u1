"""Guarda las condiciones de la máquina donde se corre el experimento.

Uso (desde la raíz del repo):
    python -m src.experimento.condiciones
    python -m src.experimento.condiciones --salida data/otro_nombre.csv

Por defecto guarda data/condiciones_<equipo>.csv, con el nombre de la
máquina, para que el CSV de un integrante no pise el de otro.

Escribe un CSV con dos columnas (campo, valor) para que el script de
análisis lo lea y arme la tabla del informe sin escribirla a mano.
"""

import argparse
import csv
import ctypes
import os
import platform
import socket
import sys
from datetime import datetime
from importlib import metadata
from pathlib import Path

# Librerías cuyas versiones interesan para el informe
PAQUETES = ["grpcio", "grpcio-tools", "protobuf", "pandas", "matplotlib", "jinja2"]


def nombre_procesador():
    """Devuelve el nombre comercial del procesador (p. ej. 'Intel Core i5-1135G7')."""
    if sys.platform == "win32":
        # En Windows, platform.processor() da algo poco útil ("Intel64 Family 6...").
        # El nombre real está en el registro.
        import winreg
        clave = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
        )
        valor, _ = winreg.QueryValueEx(clave, "ProcessorNameString")
        return valor.strip()
    if sys.platform.startswith("linux"):
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for linea in f:
                if linea.startswith("model name"):
                    return linea.split(":", 1)[1].strip()
    if sys.platform == "darwin":
        return os.popen("sysctl -n machdep.cpu.brand_string").read().strip()
    return platform.processor() or "desconocido"


def memoria_ram_gb():
    """Devuelve la RAM total en GB, redondeada a un decimal."""
    if sys.platform == "win32":
        # Estructura que pide la función GlobalMemoryStatusEx de la API de Windows
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        estado = MEMORYSTATUSEX()
        estado.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(estado))
        total_bytes = estado.ullTotalPhys
    elif sys.platform == "darwin":
        total_bytes = int(os.popen("sysctl -n hw.memsize").read())
    else:
        # Linux: páginas físicas × tamaño de página
        total_bytes = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    return round(total_bytes / 1024**3, 1)


def version_paquete(nombre):
    try:
        return metadata.version(nombre)
    except metadata.PackageNotFoundError:
        return "no instalado"


def recolectar():
    """Arma la lista de (campo, valor) con los datos de esta máquina."""
    if sys.platform == "win32":
        # platform.release() no distingue Windows 10 de 11; la build sí (11 >= 22000)
        build = int(platform.version().split(".")[-1])
        nombre_so = "Windows 11" if build >= 22000 else f"Windows {platform.release()}"
    else:
        nombre_so = f"{platform.system()} {platform.release()}"

    datos = [
        ("fecha_hora", datetime.now().isoformat(timespec="seconds")),
        ("equipo", socket.gethostname()),
        ("sistema_operativo", nombre_so),
        ("version_so", platform.version()),
        ("arquitectura", platform.machine()),
        ("procesador", nombre_procesador()),
        ("nucleos_logicos", os.cpu_count()),
        ("ram_gb", memoria_ram_gb()),
        ("python", platform.python_version()),
        ("implementacion_python", platform.python_implementation()),
        ("modo_nodos", "procesos en localhost (puertos 5000, 5001, 5002)"),
    ]
    for paquete in PAQUETES:
        datos.append((f"version_{paquete}", version_paquete(paquete)))
    return datos


def main():
    parser = argparse.ArgumentParser(description="Registra las condiciones de la máquina.")
    parser.add_argument("--salida", default=None,
                        help="ruta del CSV de salida (por defecto data/condiciones_<equipo>.csv)")
    args = parser.parse_args()

    datos = recolectar()

    if args.salida:
        salida = Path(args.salida)
    else:
        # El nombre de la máquina va en el archivo, p. ej. data/condiciones_desktop-i6fpv0i.csv
        equipo = socket.gethostname().lower()
        salida = Path("data") / f"condiciones_{equipo}.csv"
    salida.parent.mkdir(parents=True, exist_ok=True)
    with open(salida, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["campo", "valor"])
        escritor.writerows(datos)

    for campo, valor in datos:
        print(f"{campo:24} {valor}")
    print(f"\nGuardado en {salida}")


if __name__ == "__main__":
    main()
