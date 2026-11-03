# Centinela Urbano: entrega final

Universidad Mariano Gálvez de Guatemala. Facultad de Ingeniería en Sistemas de Información y
Ciencias de la Computación. Arquitectura de Computadores II, sección 040. Grupo 3.

| Integrante | Carné |
|---|---|
| Bryan Rolando Cristales Pérez | 7590-23-14719 |
| Jonathan Josue Saban Cua | 7590-23-18023 |
| Arody Rodrigo Angel Sontay | 7590-23-3295 |
| Luis Alfredo Borrayo Top | 7590-12-17101 |
| Ian Gabriel Barrera Medina | 7590-23-15170 |

## Contenido

| Entregable del enunciado | Ubicación |
|---|---|
| Informe final académico | `1-Informe-final/Informe-Final-Centinela-Urbano.docx` (y `.pdf`) |
| Documento de Architecture Decision Records (10 ADRs, formato de la sección 4.1) | `2-Anexo-ADRs/Anexo-A-ADRs.docx` (y `.pdf`) |
| Diseño de infraestructura (nodos, servicios y segmentos de red) | `3-Diseno-de-infraestructura/` |
| Documentación del sistema de correlación e inferencia | `4-Sistema-de-correlacion/` |
| Visualización en Grafana | `5-Visualizacion-Grafana/`: tableros y capturas. Grafana los carga solo desde `7-Proyecto/infra/grafana/` |
| Documentación técnica del sistema: manual de instalación y operación, y despliegue en varias PCs | `6-Manual-de-operacion/` |
| Configuración técnica de servicios y contenedores | `7-Proyecto/docker-compose.yml` (una PC), `7-Proyecto/deploy/` (un archivo por nodo), `7-Proyecto/Dockerfile`, `7-Proyecto/infra/` y el `application.yml` de cada servicio |
| Scripts de simulación de eventos | `7-Proyecto/sensor-simulator/`: generadores de los cinco sensores, escenarios de correlación y reproducción de CSV. Se ejecutan con `centinela.cmd escenario`, `pico`, `fondo` y `replay` |
| Implementación de los servicios | `7-Proyecto/` |

## Puesta en marcha en una PC

Solo se necesita Docker Desktop. En la carpeta `7-Proyecto`:

```bat
centinela.cmd configurar        :: opción 1: todo en esta PC
centinela.cmd iniciar -Compilar :: compila dentro de Docker y levanta todo
centinela.cmd validar           :: comprueba las 9 correlaciones esperadas
```

| Servicio | Dirección | Usuario |
|---|---|---|
| Consola del operador | http://localhost:8083 | operador / operador123 |
| Grafana | http://localhost:3000 | admin / admin123 |
| Vigilante de los nodos | http://localhost:8080/vigilante.html | no requiere |

El reparto en varias PCs (nodos A, B y C/D) y el guion de la puesta en simulación están en
`6-Manual-de-operacion/`.
