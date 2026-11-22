"""Verifica el reloj de Lamport con los CSV de eventos de las corridas reales.

Uso (desde la raíz del repo):
    python -m analysis.verificar_lamport
    python -m analysis.verificar_lamport --eventos "data/eventos_*_n3-r*.csv"

Lee los archivos data/eventos_<mecanismo>_<nodo>_<equipo>_<corrida>.csv que escribe
src/comun/lamport.py, con las columnas:
    corrida, mecanismo, nodo, evento, reloj, reloj_emisor, origen, destino, hora_real

Cómo empareja: una recepción guarda en reloj_emisor el timestamp que traía el
mensaje, que es el reloj del emisor justo después de enviar. Entonces su envío es
la fila con evento = envio y
    (corrida, nodo, reloj) del envío == (corrida, origen, reloj_emisor) de la recepción.
Dentro de un nodo el reloj nunca se repite, así que esa clave es única.

Qué comprueba:
  1. Toda recepción encuentra su envío, y el envío iba dirigido a ese nodo.
  2. reloj de la recepción > reloj del envío, en el 100 % de los mensajes.
  3. Todo envío tiene su recepción (si no, se perdió un mensaje).
  4. En cada nodo el reloj sube en cada fila del CSV.
  5. Busca eventos de nodos distintos con el mismo reloj en la misma corrida.
     Esos eventos son concurrentes: si uno causara al otro, su reloj sería menor.
  6. Convergencia (pregunta 1), en las corridas de intercambios (etiqueta nX-rY):
     el momento en que todos los nodos ya recibieron información, directa o
     indirecta, de todos los demás, y cuántos intercambios hicieron falta.
     Cada nodo empieza sabiendo solo de sí mismo. Un mensaje lleva lo que sabía
     el emisor al enviarlo, y al recibirlo el receptor lo suma a lo suyo. Los
     eventos se recorren en el orden total de Lamport (reloj y, si empatan,
     nombre del nodo), que respeta la causalidad: un envío siempre va antes que
     su recepción. El tiempo va desde el primer envío de la corrida hasta la
     recepción que completa la condición, con hora_real (todos los nodos corren
     en la misma máquina y leen el mismo reloj).

Guarda las tablas en analysis/ para que el informe las use tal cual.
Termina con código 1 si encuentra cualquier problema de los puntos 1 a 4.
"""

import argparse
import glob
import sys
from pathlib import Path

import pandas as pd

from src.comun.lamport import COLUMNAS


def cargar(patron):
    archivos = sorted(glob.glob(patron))
    if not archivos:
        sys.exit(f"No hay archivos que coincidan con {patron}")
    tablas = []
    for archivo in archivos:
        # Todo como texto: reloj_emisor viene vacío en los envíos
        df = pd.read_csv(archivo, dtype=str, keep_default_na=False)
        if list(df.columns) != COLUMNAS:
            sys.exit(f"{archivo}: las columnas no son las de lamport.py")
        df["archivo"] = Path(archivo).name
        df["fila"] = range(len(df))             # orden en que el nodo escribió
        tablas.append(df)
    eventos = pd.concat(tablas, ignore_index=True)
    eventos["reloj"] = eventos["reloj"].astype(int)
    return eventos, archivos


def emparejar(eventos):
    """Une cada recepción con su envío. Devuelve (pares, envíos sin recepción)."""
    envios = eventos[eventos["evento"] == "envio"]
    recepciones = eventos[eventos["evento"] == "recepcion"].copy()
    recepciones["reloj_emisor"] = recepciones["reloj_emisor"].astype(int)

    claves_envio = envios[["corrida", "nodo", "reloj", "destino", "mecanismo"]].rename(
        columns={"nodo": "origen", "reloj": "reloj_emisor",
                 "destino": "destino_envio", "mecanismo": "mecanismo_envio"})
    claves_envio["reloj_envio"] = claves_envio["reloj_emisor"]

    pares = recepciones.merge(claves_envio, on=["corrida", "origen", "reloj_emisor"],
                              how="left", indicator=True)
    pares["con_envio"] = pares["_merge"] == "both"
    pares["destino_ok"] = pares["destino_envio"] == pares["nodo"]
    pares["cumple"] = pares["con_envio"] & (pares["reloj"] > pares["reloj_envio"])

    recibidos = pares.loc[pares["con_envio"], ["corrida", "origen", "reloj_emisor"]]
    marca = envios.merge(recibidos.rename(columns={"origen": "nodo", "reloj_emisor": "reloj"}),
                         on=["corrida", "nodo", "reloj"], how="left", indicator=True)
    sin_recepcion = marca[marca["_merge"] == "left_only"]
    return pares, sin_recepcion


def relojes_que_no_suben(eventos):
    problemas = []
    for archivo, grupo in eventos.groupby("archivo"):
        relojes = grupo.sort_values("fila")["reloj"].tolist()
        for antes, despues in zip(relojes, relojes[1:]):
            if despues <= antes:
                problemas.append({"archivo": archivo, "antes": antes, "despues": despues})
    return pd.DataFrame(problemas, columns=["archivo", "antes", "despues"])


def buscar_empates(eventos):
    """Pares de eventos de nodos distintos con el mismo reloj en la misma corrida."""
    cols = ["corrida", "mecanismo", "nodo", "evento", "reloj", "origen", "destino", "hora_real"]
    e = eventos[cols]
    empates = e.merge(e, on=["corrida", "mecanismo", "reloj"], suffixes=("_a", "_b"))
    empates = empates[empates["nodo_a"] < empates["nodo_b"]]   # sin repetir ni el mismo nodo
    return empates.sort_values(["corrida", "mecanismo", "reloj"]).reset_index(drop=True)


def convergencia(eventos):
    """Por corrida de intercambios: cuándo todos los nodos saben de todos los demás."""
    filas = []
    intercambios = eventos[eventos["corrida"].str.contains(r"_n\d+-r\d+$", regex=True)]
    for (corrida, mecanismo), grupo in intercambios.groupby(["corrida", "mecanismo"]):
        nodos = sorted(grupo["nodo"].unique())
        sabe = {n: {n} for n in nodos}          # de quién tiene información cada nodo
        lleva = {}                              # (emisor, reloj) -> lo que llevaba el mensaje
        recibidos = 0
        inicio = grupo.loc[grupo["evento"] == "envio", "hora_real"].astype(float).min()
        fila = {"corrida": corrida, "mecanismo": mecanismo, "nodos": len(nodos),
                "converge": False, "intercambios": None, "reloj": None,
                "nodo_que_completa": None, "segundos": None}
        for ev in grupo.sort_values(["reloj", "nodo"]).itertuples():
            if ev.evento == "envio":
                lleva[(ev.nodo, ev.reloj)] = set(sabe[ev.nodo])
                continue
            recibidos += 1
            # Si el envío no está (mensaje sin pareja), al menos se sabe del emisor
            sabe[ev.nodo] |= lleva.get((ev.origen, int(ev.reloj_emisor)), {ev.origen})
            if all(len(s) == len(nodos) for s in sabe.values()):
                fila.update(converge=True, intercambios=recibidos, reloj=ev.reloj,
                            nodo_que_completa=ev.nodo,
                            segundos=round(float(ev.hora_real) - inicio, 6))
                break
        filas.append(fila)
    return pd.DataFrame(filas, columns=["corrida", "mecanismo", "nodos", "converge",
                                        "intercambios", "reloj", "nodo_que_completa",
                                        "segundos"])


def resumen_por_corrida(eventos, pares, sin_recepcion, empates):
    claves = ["corrida", "mecanismo"]
    r = eventos.groupby(claves).agg(nodos=("nodo", "nunique")).reset_index()
    p = pares.groupby(claves).agg(mensajes=("cumple", "size"),
                                 cumplen=("cumple", "sum"),
                                 sin_envio=("con_envio", lambda s: int((~s).sum()))).reset_index()
    r = r.merge(p, on=claves, how="left")
    r["envios_sin_recepcion"] = r.set_index(claves).index.map(
        sin_recepcion.groupby(claves).size()).fillna(0).astype(int)
    r["empates"] = r.set_index(claves).index.map(
        empates.groupby(claves).size()).fillna(0).astype(int)
    r[["mensajes", "cumplen", "sin_envio"]] = r[["mensajes", "cumplen", "sin_envio"]].fillna(0).astype(int)
    return r


def main():
    parser = argparse.ArgumentParser(description="Verifica el reloj de Lamport con los CSV reales.")
    parser.add_argument("--eventos", default="data/eventos_*.csv", help="patrón de los CSV de eventos")
    parser.add_argument("--salida", default="analysis", help="carpeta de las tablas de salida")
    args = parser.parse_args()

    eventos, archivos = cargar(args.eventos)
    pares, sin_recepcion = emparejar(eventos)
    no_suben = relojes_que_no_suben(eventos)
    empates = buscar_empates(eventos)
    resumen = resumen_por_corrida(eventos, pares, sin_recepcion, empates)
    conv = convergencia(eventos)

    malos = pares[~pares["cumple"] | ~pares["destino_ok"]]
    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)
    resumen.to_csv(salida / "lamport_resumen.csv", index=False)
    malos[["corrida", "mecanismo", "nodo", "origen", "reloj_emisor", "reloj", "con_envio",
           "destino_ok", "archivo"]].to_csv(salida / "lamport_violaciones.csv", index=False)
    empates.to_csv(salida / "lamport_empates.csv", index=False)
    conv.to_csv(salida / "lamport_convergencia.csv", index=False)

    total = len(pares)
    cumplen = int(pares["cumple"].sum())
    print(f"Archivos: {len(archivos)}   corridas: {resumen['corrida'].nunique()}")
    print(f"Mensajes recibidos: {total}   cumplen reloj_recepcion > reloj_envio: "
          f"{cumplen} ({100 * cumplen / total if total else 0:.1f} %)")

    hay_problemas = False
    if len(malos):
        hay_problemas = True
        print(f"\nVIOLACIONES: {len(malos)} recepciones sin envío, con destino equivocado "
              f"o con reloj que no sube. Detalle en {salida / 'lamport_violaciones.csv'}")
        print(malos[["corrida", "nodo", "origen", "reloj_emisor", "reloj"]].head(10).to_string(index=False))
    if len(sin_recepcion):
        hay_problemas = True
        print(f"\nEnvíos sin recepción (mensajes perdidos): {len(sin_recepcion)}")
        print(sin_recepcion[["corrida", "nodo", "reloj", "destino"]].head(10).to_string(index=False))
    if len(no_suben):
        hay_problemas = True
        print("\nRelojes que no suben dentro de un nodo:")
        print(no_suben.head(10).to_string(index=False))
    if not hay_problemas:
        print("OK: no hay violaciones de causalidad ni mensajes sin pareja.")

    if empates.empty:
        print("\nNo hay eventos de nodos distintos con el mismo reloj.")
    else:
        ej = empates.iloc[0]
        print(f"\nEventos de nodos distintos con el mismo reloj: {len(empates)} pares. Ejemplo:")
        print(f"  corrida {ej['corrida']} ({ej['mecanismo']}), reloj = {ej['reloj']}")
        for lado in ("a", "b"):
            print(f"  {ej['nodo_' + lado]}: {ej['evento_' + lado]} "
                  f"{ej['origen_' + lado]} -> {ej['destino_' + lado]}, hora_real {ej['hora_real_' + lado]}")

    if not conv.empty:
        print("\nConvergencia (todos los nodos saben de todos):")
        tabla = (conv.groupby(["mecanismo", "nodos"])
                 .agg(corridas=("corrida", "count"), convergen=("converge", "sum"),
                      intercambios_min=("intercambios", "min"),
                      intercambios_max=("intercambios", "max"),
                      segundos_mediana=("segundos", "median"))
                 .reset_index())
        print(tabla.to_string(index=False))

    sys.exit(1 if hay_problemas else 0)


if __name__ == "__main__":
    main()
