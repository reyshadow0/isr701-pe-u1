# Bitácora del grupo CLQR

PE-U1: comunicación entre procesos con sockets TCP y gRPC. Aplicaciones Distribuidas (ISR-701), UTEQ.

Cada integrante escribe solo en su sección y agrega sus entradas al final de ella. Así los PR no chocan al
fusionarse. La entrada va en el mismo commit que el trabajo que describe.

## Formato de cada entrada

- **Día dd/mm** — qué hiciste, con tus palabras.
  - Problemas y cómo los resolviste (si hubo).
  - Uso de IA: herramienta y para qué (si la usaste).

## Decisiones del grupo
- 18/11: Los nodos corren como procesos en localhost, sin Docker. El enunciado permite las dos opciones, y la red virtual de los contenedores mete variación en las latencias que vamos a medir.
- 18/11: Puertos: el nodo A usa el 5000, el B el 5001 y el C el 5002. Sockets y gRPC nunca corren a la vez.
- 18/11: Todo se ejecuta con `python -m` desde la raíz del repositorio.
- 18/11: El código de gRPC se genera desde la raíz con `python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/mensajeria.proto`. Los archivos generados quedan en `proto/`.
- 18/11: El acuse no cuenta como evento de Lamport. Solo es la respuesta del protocolo que confirma que el mensaje llegó.
- 18/11: Python 3.13 y versiones fijas en `requirements.txt`: `grpcio==1.85.3`, `grpcio-tools==1.85.3`, `protobuf==7.36.2`, `pandas==3.0.2`, `matplotlib==3.11.2` y `jinja2==3.1.6`. El archivo lo sube Dayver.
- 18/11: Reparto: Steffanny hace sockets TCP (`steffanny/sockets`), Luis hace gRPC (`luis/grpc`), Dayver el experimento y el `requirements.txt` (`dayver/experimento`) y Mario los relojes de Lamport y el análisis (`mario/lamport-analisis`).
## Castro Coello Mario Sebastián
- **Día 18/11** — Con apoyo de Claude Code preparé el repositorio y el plan de trabajo. Hice el commit inicial `7a14cc6` y el push a `main`. Reenvié la invitación de Dayver, que había vencido; a Luis y Steffanny los invité el 09/11. Mandé al grupo el reparto y el plan por día, y los tres lo aceptaron. Instalé los paquetes de Python del proyecto y repasé el reloj de Lamport y `threading.Lock` para mi parte.
  - Al probar el comando de `protoc` dio error porque el `.proto` todavía no existía. Queda pendiente hasta que Luis suba `proto/mensajeria.proto` en su rama.
  - Uso de IA: Claude Code. Lo usé para proponer el reparto y el plan por día, redactar los mensajes al grupo y a cada integrante, armar el esqueleto del repositorio, las ramas y la plantilla de `BITACORA.md`, reenviar la invitación de Dayver, probar el comando de gRPC y explicarme el reloj de Lamport y `threading.Lock`. El commit y el push los hice yo, y el reparto lo decidimos en grupo.


## Loor Suárez Steffanny del Rocío

## Quintero Rico Luis David

## Rivas Piloso Dayver Yahir

