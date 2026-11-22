"""Analiza la latencia de sockets y gRPC con los CSV crudos de data/.

Uso (desde la raíz del repo):
    python -m analysis.analizar
    python -m analysis.analizar --datos data --salida docs

Lee los archivos data/latencia_<mecanismo>_<nodo>_<equipo>_<etiqueta>.csv que
escribe src/comun/nodo.py (uno por cliente y corrida), con las columnas:
    mecanismo, escenario, tamano_bytes, corrida, nodo, envio, rtt_ms, hora_real
No lee latency_sockets.csv ni latency_grpc.csv, porque son copias de los mismos
datos y se contarían dos veces.

Unidad de análisis: la repetición. Una repetición es una corrida (columna
corrida = <equipo>_<etiqueta>) de un mecanismo. Con tres clientes se juntan los
300 envíos de la corrida. La repetición r0 es el calentamiento y se descarta.
Si la etiqueta no termina en -rN (corridas de prueba), la corrida cuenta como
una repetición y se avisa en pantalla.

Por repetición calcula: media, mediana, desviación estándar (muestral), p95
(interpolación lineal) del RTT en ms y mensajes por segundo. Los mensajes por
segundo son los envíos divididos entre la duración de la repetición, que va
desde que salió el primer envío (hora_real - rtt_ms del primero) hasta el acuse
del último (la hora_real más alta).

Genera, sin nada hecho a mano:
    docs/tablas/latencia_repeticiones.csv   métricas de cada repetición
    docs/tablas/latencia_resumen.csv/.tex   resumen por mecanismo, escenario y tamaño
    docs/tablas/hipotesis.csv/.tex          criterio de decisión de H1, H2 y H3
    docs/tablas/condiciones.tex             condiciones de la máquina (si hay CSV)
    docs/figuras/boxplot_latencia.png       RTT de todos los envíos
    docs/figuras/barras_intervalos.png      mediana y mensajes/s con IC del 95 %

Criterio de decisión (sección 1 del informe):
  * Diferencia real en una métrica: los rangos de los valores por repetición de
    los dos mecanismos no se traslapan.
  * H2 y H3 comparan los cocientes mediana_gRPC / mediana_sockets de todas las
    combinaciones de repeticiones (5 x 5 = 25 por condición).
"""

import argparse
import re
import sys
from itertools import product
from pathlib import Path

import matplotlib

matplotlib.use("Agg")                     # sin ventana: solo guarda los PNG
import matplotlib.pyplot as plt           # noqa: E402
import pandas as pd                       # noqa: E402

COLUMNAS = ["mecanismo", "escenario", "tamano_bytes", "corrida", "nodo",
            "envio", "rtt_ms", "hora_real"]
MECANISMOS = ["sockets", "grpc"]
NOMBRE_MEC = {"sockets": "Sockets TCP + JSON", "grpc": "gRPC + Protobuf"}
COLOR_MEC = {"sockets": "#2a78d6", "grpc": "#eb6834"}
ESCENARIOS = ["secuencial", "concurrente"]
NOMBRE_ESC = {"secuencial": "1 cliente", "concurrente": "3 clientes"}
CLAVE = ["mecanismo", "escenario", "tamano_bytes"]

# t de Student de dos colas al 95 % según los grados de libertad (n - 1)
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365,
       8: 2.306, 9: 2.262, 10: 2.228, 15: 2.131, 20: 2.086, 30: 2.042}


def t95(gl):
    if gl > 30:
        return 1.96
    return T95[max(k for k in T95 if k <= gl)]


def nombre_tamano(b):
    return f"{b // 1024} KB" if b >= 1024 else f"{b} B"


# ----------------------------------------------------------------- lectura

def cargar(datos):
    archivos = sorted(Path(datos).glob("latencia_*.csv"))
    if not archivos:
        sys.exit(f"No hay archivos latencia_*.csv en {datos}/")
    tablas = []
    for archivo in archivos:
        df = pd.read_csv(archivo)
        if list(df.columns) != COLUMNAS:
            sys.exit(f"{archivo.name}: las columnas no son {COLUMNAS}")
        if df.empty:
            sys.exit(f"{archivo.name}: no tiene envíos")
        df["archivo"] = archivo.name
        tablas.append(df)
    envios = pd.concat(tablas, ignore_index=True)

    desconocidos = set(envios["mecanismo"]) - set(MECANISMOS)
    if desconocidos:
        sys.exit(f"Mecanismos desconocidos: {sorted(desconocidos)}")

    # corrida = <equipo>_<etiqueta>; la repetición sale del final de la etiqueta
    envios["etiqueta"] = envios["corrida"].str.split("_", n=1).str[-1]
    rep = envios["etiqueta"].str.extract(r"-r(\d+)$", expand=False)
    envios["repeticion"] = pd.to_numeric(rep)            # NaN = corrida de prueba
    return envios, archivos


def descartar_calentamiento(envios):
    prueba = envios.loc[envios["repeticion"].isna(), "corrida"].unique()
    if len(prueba):
        print(f"Aviso: {len(prueba)} corrida(s) sin -rN en la etiqueta; cada una "
              f"cuenta como una repetición: {', '.join(sorted(prueba))}")
    calentamiento = envios["repeticion"] == 0
    print(f"Calentamiento descartado (r0): {calentamiento.sum()} envíos")
    return envios[~calentamiento].copy()


# ----------------------------------------------------------------- métricas

def por_repeticion(envios):
    filas = []
    for (mec, esc, tam, corrida), g in envios.groupby(CLAVE + ["corrida"]):
        rtt = g["rtt_ms"]
        inicio = (g["hora_real"] - g["rtt_ms"] / 1000).min()
        duracion = g["hora_real"].max() - inicio
        filas.append({
            "mecanismo": mec, "escenario": esc, "tamano_bytes": tam, "corrida": corrida,
            "clientes": g["nodo"].nunique(), "envios": len(g),
            "media_ms": rtt.mean(), "mediana_ms": rtt.median(),
            "desv_ms": rtt.std(ddof=1), "p95_ms": rtt.quantile(0.95),
            "duracion_s": duracion, "msg_s": len(g) / duracion,
        })
    return pd.DataFrame(filas)


def intervalo(serie):
    """Media e intervalo de confianza del 95 % (t de Student). Con n = 1 no hay IC."""
    n = len(serie)
    media = serie.mean()
    if n < 2:
        return media, float("nan")
    return media, t95(n - 1) * serie.std(ddof=1) / n ** 0.5


def resumen(envios, reps):
    filas = []
    for (mec, esc, tam), g in envios.groupby(CLAVE):
        r = reps[(reps["mecanismo"] == mec) & (reps["escenario"] == esc)
                 & (reps["tamano_bytes"] == tam)]
        msg, ic_msg = intervalo(r["msg_s"])
        med, ic_med = intervalo(r["mediana_ms"])
        rtt = g["rtt_ms"]
        filas.append({
            "mecanismo": mec, "escenario": esc, "tamano_bytes": tam,
            "repeticiones": len(r), "envios": len(g),
            "media_ms": rtt.mean(), "mediana_ms": rtt.median(),
            "desv_ms": rtt.std(ddof=1), "p95_ms": rtt.quantile(0.95),
            "mediana_rep_ms": med, "ic95_mediana_rep_ms": ic_med,
            "msg_s": msg, "ic95_msg_s": ic_msg,
        })
    df = pd.DataFrame(filas)
    return ordenar(df)


def ordenar(df):
    df = df.copy()
    df["_e"] = df["escenario"].map({e: i for i, e in enumerate(ESCENARIOS)})
    df["_m"] = df["mecanismo"].map({m: i for i, m in enumerate(MECANISMOS)})
    return df.sort_values(["_e", "tamano_bytes", "_m"]).drop(columns=["_e", "_m"])


# ----------------------------------------------------------------- hipótesis

def valores(reps, mec, esc, tam, metrica):
    sel = reps[(reps["mecanismo"] == mec) & (reps["escenario"] == esc)
               & (reps["tamano_bytes"] == tam)]
    return list(sel[metrica])


def sockets_mejor(reps, esc, tam, metrica):
    """True/False si hay o no diferencia real a favor de sockets; None si faltan datos."""
    s = valores(reps, "sockets", esc, tam, metrica)
    g = valores(reps, "grpc", esc, tam, metrica)
    if not s or not g:
        return None
    if metrica == "msg_s":                    # más es mejor
        return min(s) > max(g)
    return max(s) < min(g)                    # menos latencia es mejor


def cocientes(reps, esc, tam):
    s = valores(reps, "sockets", esc, tam, "mediana_ms")
    g = valores(reps, "grpc", esc, tam, "mediana_ms")
    return [gi / si for gi, si in product(g, s)]


def texto(v):
    return {True: "sí", False: "no", None: "sin datos"}[v]


def hipotesis(reps):
    filas = []

    def todas(*vs):
        return None if any(v is None for v in vs) else all(vs)

    # H1: 100 B, un cliente
    h1 = {m: sockets_mejor(reps, "secuencial", 100, m)
          for m in ("mediana_ms", "p95_ms", "msg_s")}
    filas.append({"hipotesis": "H1", "condicion": "100 B, 1 cliente",
                  "detalle": (f"mediana menor: {texto(h1['mediana_ms'])}; "
                              f"p95 menor: {texto(h1['p95_ms'])}; "
                              f"más msg/s: {texto(h1['msg_s'])}"),
                  "se_cumple": texto(todas(*h1.values()))})

    # H2: 10 KB, un cliente; ventaja relativa menor que con 100 B
    c100 = cocientes(reps, "secuencial", 100)
    c10k = cocientes(reps, "secuencial", 10240)
    med = sockets_mejor(reps, "secuencial", 10240, "mediana_ms")
    menor = (max(c10k) < min(c100)) if c100 and c10k else None
    filas.append({"hipotesis": "H2", "condicion": "10 KB vs 100 B, 1 cliente",
                  "detalle": (f"mediana menor con 10 KB: {texto(med)}; "
                              f"cocientes 10 KB {rango(c10k)} "
                              f"< cocientes 100 B {rango(c100)}: {texto(menor)}"),
                  "se_cumple": texto(todas(med, menor))})

    # H3: tres clientes, 100 B; ventaja relativa mayor que con un cliente
    c3 = cocientes(reps, "concurrente", 100)
    h3 = {m: sockets_mejor(reps, "concurrente", 100, m) for m in ("mediana_ms", "p95_ms")}
    mayor = (min(c3) > max(c100)) if c100 and c3 else None
    filas.append({"hipotesis": "H3", "condicion": "100 B, 3 clientes vs 1",
                  "detalle": (f"mediana menor: {texto(h3['mediana_ms'])}; "
                              f"p95 menor: {texto(h3['p95_ms'])}; "
                              f"cocientes 3 clientes {rango(c3)} "
                              f"> cocientes 1 cliente {rango(c100)}: {texto(mayor)}"),
                  "se_cumple": texto(todas(*h3.values(), mayor))})
    return pd.DataFrame(filas)


def rango(xs):
    return f"[{min(xs):.2f}; {max(xs):.2f}]" if xs else "[sin datos]"


# ----------------------------------------------------------------- tablas LaTeX

def escapar(s):
    return str(s).replace("%", r"\%").replace("_", r"\_").replace("&", r"\&")


def num(x, dec):
    return "--" if pd.isna(x) else f"{x:.{dec}f}"


def tabla_latex(encabezado, filas, formato):
    lineas = [r"\begin{tabular}{" + formato + "}", r"\toprule",
              " & ".join(encabezado) + r" \\", r"\midrule"]
    lineas += [" & ".join(f) + r" \\" for f in filas]
    lineas += [r"\bottomrule", r"\end{tabular}", ""]
    return "\n".join(lineas)


def latex_resumen(res):
    encabezado = ["Mecanismo", "Clientes", "Tamaño", "Rep.", "Envíos", "Media", "Mediana",
                  "Desv. est.", "P95", "Msg/s", r"IC 95\,\% msg/s"]
    filas = []
    for _, r in res.iterrows():
        ic = r["ic95_msg_s"]
        filas.append([NOMBRE_MEC[r["mecanismo"]], NOMBRE_ESC.get(r["escenario"], r["escenario"]),
                      nombre_tamano(r["tamano_bytes"]), str(r["repeticiones"]), str(r["envios"]),
                      num(r["media_ms"], 3), num(r["mediana_ms"], 3), num(r["desv_ms"], 3),
                      num(r["p95_ms"], 3), num(r["msg_s"], 1),
                      "--" if pd.isna(ic) else r"$\pm$" + num(ic, 1)])
    return ("% Generado por analysis/analizar.py. No editar a mano.\n"
            "% Latencias en ms (RTT). Msg/s: media de las repeticiones.\n"
            + tabla_latex(encabezado, filas, "llrrrrrrrrr"))


def latex_hipotesis(hip):
    filas = [[h["hipotesis"], escapar(h["condicion"]), escapar(h["detalle"]), h["se_cumple"]]
             for _, h in hip.iterrows()]
    return ("% Generado por analysis/analizar.py. No editar a mano.\n"
            + tabla_latex(["Hip.", "Condición", "Detalle", "¿Se cumple?"], filas,
                          "l l p{8cm} l"))


def latex_condiciones(datos):
    archivos = sorted(Path(datos).glob("condiciones_*.csv"))
    if not archivos:
        return None
    partes = ["% Generado por analysis/analizar.py. No editar a mano."]
    for archivo in archivos:
        df = pd.read_csv(archivo, dtype=str, keep_default_na=False)
        filas = [[escapar(c), escapar(v)] for c, v in zip(df["campo"], df["valor"])]
        partes.append(f"% {archivo.name}")
        partes.append(tabla_latex(["Campo", "Valor"], filas, "l l"))
    return "\n".join(partes)


# ----------------------------------------------------------------- figuras

def condiciones_presentes(envios):
    pares = envios[["escenario", "tamano_bytes"]].drop_duplicates()
    pares = pares.assign(_e=pares["escenario"].map({e: i for i, e in enumerate(ESCENARIOS)}))
    pares = pares.sort_values(["_e", "tamano_bytes"])
    return list(zip(pares["escenario"], pares["tamano_bytes"]))


def estilo(ax):
    for lado in ("top", "right"):
        ax.spines[lado].set_visible(False)
    ax.spines["left"].set_color("#b5b4ad")
    ax.spines["bottom"].set_color("#b5b4ad")
    ax.tick_params(colors="#52514e", labelsize=9)
    ax.grid(axis="y", color="#e4e3df", linewidth=0.8)
    ax.set_axisbelow(True)


def figura_boxplot(envios, ruta):
    conds = condiciones_presentes(envios)
    fig, ejes = plt.subplots(1, len(conds), figsize=(3.2 * len(conds), 4),
                             sharey=True, squeeze=False)
    for ax, (esc, tam) in zip(ejes[0], conds):
        mecs = [m for m in MECANISMOS
                if ((envios["mecanismo"] == m) & (envios["escenario"] == esc)
                    & (envios["tamano_bytes"] == tam)).any()]
        datos = [envios.loc[(envios["mecanismo"] == m) & (envios["escenario"] == esc)
                            & (envios["tamano_bytes"] == tam), "rtt_ms"] for m in mecs]
        cajas = ax.boxplot(datos, widths=0.55, patch_artist=True,
                           medianprops={"color": "#0b0b0b", "linewidth": 1.5},
                           flierprops={"marker": "o", "markersize": 3, "alpha": 0.5,
                                       "markeredgecolor": "#52514e"})
        for caja, m in zip(cajas["boxes"], mecs):
            caja.set_facecolor(COLOR_MEC[m])
            caja.set_alpha(0.85)
            caja.set_edgecolor("#52514e")
        ax.set_xticks(range(1, len(mecs) + 1))
        ax.set_xticklabels([NOMBRE_MEC[m].split(" +")[0] for m in mecs])
        ax.set_title(f"{NOMBRE_ESC.get(esc, esc)}, {nombre_tamano(tam)}", fontsize=10)
        ax.set_yscale("log")
        estilo(ax)
    ejes[0][0].set_ylabel("RTT (ms, escala logarítmica)")
    fig.suptitle("Latencia de ida y vuelta por envío (sin calentamiento)", fontsize=11)
    fig.tight_layout()
    fig.savefig(ruta, dpi=200)
    plt.close(fig)


def figura_barras(res, ruta):
    conds = list(dict.fromkeys(zip(res["escenario"], res["tamano_bytes"])))
    paneles = [("mediana_rep_ms", "ic95_mediana_rep_ms", "Mediana del RTT (ms)"),
               ("msg_s", "ic95_msg_s", "Mensajes por segundo")]
    fig, ejes = plt.subplots(1, 2, figsize=(11, 4))
    ancho = 0.38
    for ax, (col, col_ic, titulo) in zip(ejes, paneles):
        for i, m in enumerate(MECANISMOS):
            xs, ys, err = [], [], []
            for j, (esc, tam) in enumerate(conds):
                fila = res[(res["mecanismo"] == m) & (res["escenario"] == esc)
                           & (res["tamano_bytes"] == tam)]
                if fila.empty:
                    continue
                xs.append(j + (i - 0.5) * ancho)
                ys.append(fila[col].iloc[0])
                err.append(0 if pd.isna(fila[col_ic].iloc[0]) else fila[col_ic].iloc[0])
            if xs:
                ax.bar(xs, ys, width=ancho - 0.03, color=COLOR_MEC[m], label=NOMBRE_MEC[m],
                       yerr=err, capsize=4, error_kw={"ecolor": "#0b0b0b", "linewidth": 1})
        ax.set_xticks(range(len(conds)))
        ax.set_xticklabels([f"{NOMBRE_ESC.get(e, e)}\n{nombre_tamano(t)}" for e, t in conds])
        ax.set_title(titulo, fontsize=10)
        estilo(ax)
    ejes[0].legend(frameon=False, fontsize=9)
    fig.suptitle("Media de las repeticiones con intervalo de confianza del 95 %", fontsize=11)
    fig.tight_layout()
    fig.savefig(ruta, dpi=200)
    plt.close(fig)


# ----------------------------------------------------------------- principal

def main():
    parser = argparse.ArgumentParser(description="Estadísticas y gráficos de latencia.")
    parser.add_argument("--datos", default="data", help="carpeta con los CSV crudos")
    parser.add_argument("--salida", default="docs", help="carpeta del informe")
    args = parser.parse_args()

    envios, archivos = cargar(args.datos)
    print(f"Leídos {len(archivos)} archivos, {len(envios)} envíos")
    envios = descartar_calentamiento(envios)
    if envios.empty:
        sys.exit("No quedan envíos después de descartar el calentamiento")

    reps = ordenar(por_repeticion(envios))
    res = resumen(envios, reps)
    hip = hipotesis(reps)

    tablas = Path(args.salida) / "tablas"
    figuras = Path(args.salida) / "figuras"
    tablas.mkdir(parents=True, exist_ok=True)
    figuras.mkdir(parents=True, exist_ok=True)

    reps.to_csv(tablas / "latencia_repeticiones.csv", index=False, float_format="%.4f")
    res.to_csv(tablas / "latencia_resumen.csv", index=False, float_format="%.4f")
    hip.to_csv(tablas / "hipotesis.csv", index=False)
    (tablas / "latencia_resumen.tex").write_text(latex_resumen(res), encoding="utf-8")
    (tablas / "hipotesis.tex").write_text(latex_hipotesis(hip), encoding="utf-8")
    cond = latex_condiciones(args.datos)
    if cond:
        (tablas / "condiciones.tex").write_text(cond, encoding="utf-8")

    figura_boxplot(envios, figuras / "boxplot_latencia.png")
    figura_barras(res, figuras / "barras_intervalos.png")

    with pd.option_context("display.width", 200, "display.max_columns", 20):
        print("\nResumen (RTT en ms):")
        print(res[["mecanismo", "escenario", "tamano_bytes", "repeticiones", "envios",
                   "media_ms", "mediana_ms", "desv_ms", "p95_ms", "msg_s", "ic95_msg_s"]]
              .round(3).to_string(index=False))
        print("\nHipótesis:")
        print(hip.to_string(index=False))
    print(f"\nTablas en {tablas}/ y figuras en {figuras}/")


if __name__ == "__main__":
    main()
