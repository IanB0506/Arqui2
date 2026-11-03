# Comandos de la demostración final

Todos se ejecutan en `cmd`, dentro de la carpeta del proyecto. Junto a cada título se indica en qué PC
va: **A**, **B**, **C** o **cualquiera** (cualquier PC configurada). `IP_A`, `IP_B` e `IP_C` son las IP
de cada nodo (con el hotspot de Windows en A, `IP_A` = 192.168.137.1).

## 1. Arranque y verificación

**1.1 Iniciar el nodo A (datos)** · PC A
```bat
centinela.cmd iniciar
```

**1.2 Iniciar el nodo B (servicios)** · PC B
```bat
centinela.cmd iniciar
```

**1.3 Iniciar el nodo C (simulador y workers)** · PC C
```bat
centinela.cmd iniciar
```

**1.4 Ver contenedores, salud de los nodos y huella del .env** · cualquiera
```bat
centinela.cmd estado
```

**1.5 Abrir la consola, Grafana y el vigilante** · cualquiera
```bat
centinela.cmd abrir
```

## 2. Despliegue de la arquitectura

**2.1 Reparto de particiones entre las máquinas** · cualquiera
```bat
centinela.cmd particiones
```

**2.2 Listar los topics de Kafka** · PC A
```bat
docker exec centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
```

**2.3 Particiones y retención de un topic** · PC A
```bat
docker exec centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic events.normalized
```

## 3. Flujo de eventos en tiempo real

**3.1 Encender el tráfico de fondo** · cualquiera
```bat
centinela.cmd fondo on
```

**3.2 Pico de carga ×20 durante 60 s** · cualquiera
```bat
centinela.cmd pico -Multiplicador 20 -Segundos 60
```

**3.3 Replay del CSV "noche de viernes"** · cualquiera
```bat
centinela.cmd replay
```

**3.4 Ver JSON crudos de cámaras LPR** · PC A
```bat
docker exec centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic sensor.lpr.raw --max-messages 3
```

**3.5 Esperar un pánico en Kafka (ventana 1)** · PC A
```bat
docker exec centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic sensor.panic.raw
```

**3.6 Enviar el pánico desde la "app" (ventana 2)** · cualquiera
```bat
centinela.cmd panico -Zona 6 -Tipo ROBO
```

**3.7 Ver el evento normalizado y seudonimizado** · PC A
```bat
docker exec centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic events.normalized --max-messages 1
```

**3.8 Ver las alertas en Kafka en vivo** · PC A
```bat
docker exec centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic alerts
```

**3.9 Apagar el tráfico de fondo** · cualquiera
```bat
centinela.cmd fondo off
```

## 4. Validación de correlaciones

**4.1 Listar los 10 escenarios** · cualquiera
```bat
centinela.cmd escenarios
```

**4.2 Ejemplo exacto de la tabla 2.4 (robo CRITICAL y falso positivo)** · cualquiera
```bat
centinela.cmd escenario tabla-2-4
```

**4.3 Robo en curso con vehículo en fuga** · cualquiera
```bat
centinela.cmd escenario robo-en-curso
```

**4.4 Accidente vial** · cualquiera
```bat
centinela.cmd escenario accidente-vial
```

**4.5 Disparos confirmados** · cualquiera
```bat
centinela.cmd escenario disparos
```

**4.6 Incendio estructural** · cualquiera
```bat
centinela.cmd escenario incendio
```

**4.7 Vehículo con reporte de robo** · cualquiera
```bat
centinela.cmd escenario vehiculo-robado
```

**4.8 Emergencia médica** · cualquiera
```bat
centinela.cmd escenario emergencia-medica
```

**4.9 Pánico sin corroborar (falso positivo)** · cualquiera
```bat
centinela.cmd escenario falso-positivo
```

**4.10 Dispositivo que abusa del botón de pánico** · cualquiera
```bat
centinela.cmd escenario boton-abusivo
```

**4.11 Pico de carga con robo simultáneo** · cualquiera
```bat
centinela.cmd escenario pico-de-carga
```

**4.12 Reporte ciudadano con texto libre y placa (dato no estructurado)** · cualquiera
```bat
centinela.cmd reporte "Choque fuerte en la esquina, un carro placa P-512KLM se dio a la fuga" -Zona 7 -Tipo ACCIDENTE
```

**4.13 Validación automática (9 de 9, unos 2,5 min)** · cualquiera
```bat
centinela.cmd validar
```

**4.14 Recalcular ya los umbrales adaptativos** · cualquiera
```bat
centinela.cmd tarea rule-quality
```

## 5. Tolerancia a fallos

**5.1 Caída del nodo B** · PC B
```bat
centinela.cmd caida B
```

**5.2 Detección con B caído (la hace el worker de C)** · cualquiera
```bat
centinela.cmd escenario disparos
```

**5.3 Recuperar el nodo B** · PC B
```bat
centinela.cmd recuperar B
```

**5.4 Caída del nodo A (Kafka, base y Redis)** · PC A
```bat
centinela.cmd caida A
```

**5.5 Robo durante la caída de A (queda en el spool)** · cualquiera
```bat
centinela.cmd escenario robo-en-curso
```

**5.6 Pánico de la app con A caído (responde EN_COLA)** · cualquiera
```bat
centinela.cmd panico -Zona 6 -Tipo ROBO
```

**5.7 Recuperar el nodo A (el spool se vacía)** · PC A
```bat
centinela.cmd recuperar A
```

**5.8 Caída del nodo C** · PC C
```bat
centinela.cmd caida C
```

**5.9 Ver que B tomó todas las particiones** · cualquiera
```bat
centinela.cmd particiones
```

**5.10 Recuperar el nodo C** · PC C
```bat
centinela.cmd recuperar C
```

## 6. JSON inválidos y DLQ

**6.1 Abrir un productor para inyectar un JSON a mano** · PC A
```bat
docker exec -it centinela-nodo-a-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic sensor.panic.raw
```

**6.2 JSON inválido para pegar (luego Enter y Ctrl+C)** · PC A, dentro del productor
```json
{"tipo_alerta":"ROBO","zona_geografica":{"nombre":"Zona 3"}}
```

**6.3 Ver la DLQ con la causa de cada mensaje** · cualquiera
```bat
centinela.cmd dlq
```

**6.4 Reprocesar la DLQ** · cualquiera
```bat
centinela.cmd reprocesar-dlq
```

## 7. Otras comprobaciones

**7.1 Registros de un servicio** · la PC donde corre el servicio
```bat
centinela.cmd logs correlation-engine
```

**7.2 Respaldo inmediato de la base** · PC A
```bat
centinela.cmd respaldo
```

**7.3 Pruebas automatizadas (la primera vez necesita internet)** · cualquiera
```bat
centinela.cmd pruebas
```

**7.4 Limpiar el estado temporal del motor (pide confirmación)** · cualquiera
```bat
centinela.cmd reiniciar-correlacion
```

**7.5 Detener al terminar (primero C, luego B y al final A)** · cada PC
```bat
centinela.cmd detener
```

## 8. Direcciones para abrir en el navegador

| Título | Dirección |
|---|---|
| Consola del operador | `http://IP_B:8083` |
| Grafana | `http://IP_B:3000` |
| Objetivos de Prometheus | `http://IP_B:9090/targets` |
| Vigilante del nodo B | `http://IP_C:8080/vigilante.html` |
| Estado del gateway de la app | `http://IP_B:8081/api/v1/gateway/status` |
| Spool del simulador | `http://IP_C:8080/api/simulator/spool` |

## 9. Plan B: una sola PC

**9.1 Configurar todo en esta PC (elegir la opción 1)** · la PC de respaldo
```bat
centinela.cmd configurar
```

**9.2 Iniciar todo** · la PC de respaldo
```bat
centinela.cmd iniciar
```

**9.3 Ver JSON en Kafka en el modo de una sola PC** · la PC de respaldo
```bat
docker exec centinela-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor.lpr.raw --max-messages 3
```

Los demás comandos (`escenario`, `validar`, `panico`, `caida`, `dlq`…) son los mismos y se ejecutan en
esa misma PC.
