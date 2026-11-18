# PE-U1 — Comunicación entre procesos con sockets TCP y gRPC

Práctica experimental de la Unidad 1 · Aplicaciones Distribuidas (ISR-701) · Universidad Técnica Estatal de
Quevedo · SPA 2026–2027

**Grupo CLQR:**

| Integrante | GitHub |
|---|---|
| Castro Coello Mario Sebastián | [`reyshadow0`](https://github.com/reyshadow0) |
| Loor Suárez Steffanny del Rocío | [`SteffannyLoorSuarez1`](https://github.com/SteffannyLoorSuarez1) |
| Quintero Rico Luis David | [`Kinetro`](https://github.com/Kinetro) |
| Rivas Piloso Dayver Yahir | [`drivasp`](https://github.com/drivasp) |

## Contenido del repositorio

| Ruta | Descripción |
|---|---|
| `src/sockets/` | Nodos y clientes con sockets TCP |
| `src/grpc/` | Nodos y clientes con gRPC |
| `proto/` | Definición del servicio en Protocol Buffers |
| `data/` | CSV crudos de las corridas y `SHA256SUMS` |
| `analysis/` | Script de análisis (estadísticas, tablas y gráficos) |
| `docs/` | Informe en LaTeX (`main.tex`, `secciones/`, `referencias.bib`, `figuras/`) y `main.pdf` |

## Compilación del informe

- **Compilador:** `pdflatex`
- **Bibliografía:** `biber` (paquete `biblatex` con estilo `ieee`)
- **Archivo principal:** `docs/main.tex`
- **Distribución usada:** MiKTeX 26.5 en Windows

Orden de comandos, ejecutados dentro de la carpeta `docs/`:

```bash
cd docs
pdflatex main.tex
biber main
pdflatex main.tex
pdflatex main.tex
```

El resultado es `docs/main.pdf`.

### Dependencias

Paquetes de LaTeX: `fontenc`, `inputenc`, `babel` (idioma `spanish`), `lmodern`, `microtype`,
`geometry`, `graphicx`, `booktabs`, `tabularx`, `longtable`, `array`, `float`, `xcolor`, `listings`,
`csquotes`, `biblatex` con `biblatex-ieee`, y `hyperref`. Además se necesita el programa `biber`.
MiKTeX instala automáticamente los paquetes que falten; en TeX Live vienen incluidos en el
esquema completo (`scheme-full`).
