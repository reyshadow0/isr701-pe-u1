# PE-U1: Comunicación entre procesos con sockets TCP y gRPC

Práctica experimental de la Unidad 1 de Aplicaciones Distribuidas (ISR-701), Universidad Técnica Estatal de
Quevedo, SPA 2026–2027.

Comparamos la latencia de ida y vuelta y los mensajes por segundo de sockets TCP con JSON y de gRPC con
Protocol Buffers, con tres nodos corriendo como procesos en `localhost`. También verificamos los relojes de
Lamport de los dos mecanismos y su convergencia con 2, 3 y 4 nodos. El informe completo está en `docs/main.pdf`.

**Grupo CLQR**

| Integrante | GitHub |
|---|---|
| Castro Coello Mario Sebastián | [`reyshadow0`](https://github.com/reyshadow0) |
| Loor Suárez Steffanny del Rocío | [`SteffannyLoorSuarez1`](https://github.com/SteffannyLoorSuarez1) |
| Quintero Rico Luis David | [`Kinetro`](https://github.com/Kinetro) |
| Rivas Piloso Dayver Yahir | [`drivasp`](https://github.com/drivasp) |

## Qué hay en el repositorio

| Ruta | Qué tiene |
|---|---|
| `src/sockets/` | Nodo con sockets TCP: servidor y cliente en un solo proceso |
| `src/grpc/` | Nodo con gRPC: servidor y cliente en un solo proceso |
| `src/comun/` | Reloj de Lamport (`lamport.py`) y lógica común de los nodos (`nodo.py`) |
| `src/experimento/` | Script que corre todo el protocolo y registro de las condiciones de la máquina |
| `proto/` | `messaging.proto` y el código generado de gRPC |
| `analysis/` | Análisis de latencia (`analizar.py`) y verificación de Lamport (`verificar_lamport.py`) |
| `data/` | CSV crudos de la corrida oficial, sin editar, y `SHA256SUMS` |
| `evidencias/` | Capturas de los nodos corriendo y del análisis |
| `tests/` | Pruebas unitarias |
| `docs/` | Informe en LaTeX y `main.pdf` |

## Preparar el entorno

Se necesita Python 3.13. Todos los comandos se ejecutan desde la raíz del repositorio con `python -m`.

```bash
pip install -r requirements.txt
```

El código de gRPC ya está generado en `proto/`. Si se cambia `messaging.proto`, se vuelve a generar con:

```bash
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/messaging.proto
```

Para correr las pruebas unitarias hace falta `pytest`:

```bash
pip install pytest
python -m pytest
```

## Ejecutar los nodos a mano

Cada nodo es un proceso que abre su servidor y también envía. Para ver los 20 intercambios de Lamport con tres
nodos, abre tres terminales y en cada una corre un nodo, con menos de 10 segundos entre uno y otro (los nodos
reintentan la conexión durante ese tiempo). Ejemplo con sockets:

```bash
python -m src.sockets.client --nodo nodo-A --puerto 5000 --corrida prueba1 --equipo tunombre --pares nodo-A=5000,nodo-B=5001,nodo-C=5002 --modo lamport --mensajes 20 --datos pruebas
python -m src.sockets.client --nodo nodo-B --puerto 5001 --corrida prueba1 --equipo tunombre --pares nodo-A=5000,nodo-B=5001,nodo-C=5002 --modo lamport --mensajes 20 --datos pruebas
python -m src.sockets.client --nodo nodo-C --puerto 5002 --corrida prueba1 --equipo tunombre --pares nodo-A=5000,nodo-B=5001,nodo-C=5002 --modo lamport --mensajes 20 --datos pruebas
```

Para gRPC es lo mismo cambiando `src.sockets.client` por `src.grpc.client_grpc`. Cada nodo termina con código 0
si envió y recibió todos sus mensajes. Los eventos quedan en `pruebas/`; usamos esa carpeta para que las
pruebas no se mezclen con los datos oficiales de `data/`. Si se repite una prueba, hay que cambiar el valor de
`--corrida`, porque un nodo no arranca si su CSV ya existe.

## Correr el experimento completo

```bash
python -m src.experimento.correr_experimento --dry-run
python -m src.experimento.correr_experimento --equipo tunombre
```

El primer comando solo muestra el plan (78 corridas) sin ejecutar nada. El segundo corre la latencia con uno y
tres clientes, con mensajes de 100 bytes y 10 KB (calentamiento `r0` y repeticiones `r1` a `r5`), y los 20
intercambios de Lamport con 2, 3 y 4 nodos. Deja los CSV en `data/`, las condiciones de la máquina en
`data/condiciones_<equipo>.csv`, el resumen de corridas en `data/corridas_<equipo>_<fecha>.csv` y la salida de
cada proceso en `logs/`. Los puertos 5000 a 5003 tienen que estar libres.

La corrida oficial del informe se hizo una sola vez, el 22/11/2026, con el equipo `mcastro`.

## Analizar los datos

```bash
python -m analysis.verificar_lamport
python -m analysis.analizar
```

`verificar_lamport` revisa que en cada mensaje el reloj de la recepción sea mayor que el del envío, busca
eventos concurrentes con el mismo reloj y calcula la convergencia; guarda sus tablas en `analysis/`.
`analizar` calcula media, mediana, desviación estándar, p95 y mensajes por segundo de cada repetición, aplica
el criterio de decisión de las hipótesis y genera las tablas de `docs/tablas/` y las figuras
`docs/figuras/boxplot.png` y `docs/figuras/barras.png`. Hay que correr primero `verificar_lamport`, porque
`analizar` usa su tabla de convergencia.

Para comprobar que los CSV de `data/` no cambiaron desde la corrida oficial:

```bash
cd data
sha256sum -c SHA256SUMS
```

En Windows, sin `sha256sum`:

```bash
python -c "import hashlib,pathlib; d=pathlib.Path('data'); m=[l.split('  ') for l in (d/'SHA256SUMS').read_text().splitlines()]; print(len(m), 'archivos;', 'todo coincide' if all(hashlib.sha256((d/n).read_bytes()).hexdigest()==h for h,n in m) else 'HAY DIFERENCIAS')"
```

## Compilar el informe

- **Compilador:** `pdflatex`
- **Bibliografía:** `biber` (paquete `biblatex` con estilo `ieee`)
- **Archivo principal:** `docs/main.tex`
- **Distribución usada:** MiKTeX 26.5 en Windows

Dentro de la carpeta `docs/`:

```bash
cd docs
pdflatex main.tex
biber main
pdflatex main.tex
pdflatex main.tex
```

El resultado es `docs/main.pdf`. Las tablas y figuras que usa el informe salen de `analysis/analizar.py`, así
que, si se vuelve a correr el análisis, hay que compilar de nuevo.

### Paquetes de LaTeX

`fontenc`, `inputenc`, `babel` (idioma `spanish`), `lmodern`, `microtype`, `geometry`, `graphicx`, `booktabs`,
`tabularx`, `longtable`, `array`, `float`, `xcolor`, `listings`, `csquotes`, `biblatex` con `biblatex-ieee`, y
`hyperref`, además del programa `biber`. MiKTeX instala solo los paquetes que falten; en TeX Live vienen en el
esquema completo (`scheme-full`).
