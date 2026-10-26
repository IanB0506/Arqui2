# Reglas de correlación editables sin recompilar

Los contenedores del motor de correlación (nodo B, *workers* de C/D y modo de una sola PC) leen
esta carpeta como configuración adicional (`/config/` dentro del contenedor). Mientras no exista
aquí un `application.yml`, se usan las reglas incluidas en la imagen
(`correlation-engine/src/main/resources/application.yml`).

Para cambiar una regla (umbral, pesos, ventanas o un tipo de evidencia):

1. Copie a `infra/correlation/application.yml` **la sección `correlation:` completa** del archivo
   original. Una lista se reemplaza entera: si el archivo trae `rules:`, debe traer todas las reglas.
2. Edite los valores. Por ejemplo, para que un robo exija más evidencia:
   ```yaml
   correlation:
     rules:
       - id: R1-ROBO
         threshold: 0.80
         # ... resto de la regla y de las demás reglas, igual que en el original
   ```
3. Copie el mismo archivo a todas las PC que corren el motor (nodo B y los nodos C/D con
   `WORKERS=true`): todas las instancias deben aplicar las mismas reglas.
4. Reinicie el motor en cada una:
   `docker restart centinela-nodo-b-correlation-engine-1` (nodo B) y
   `docker restart centinela-nodo-c-correlation-worker-1` (nodo C/D con *workers*).
   Con una sola PC: `docker restart centinela-correlation-engine-1`.

Al arrancar, el motor valida cada regla (id, tipo de alerta y ancla obligatorios, umbrales entre 0
y 1, ancla con al menos un tipo de sensor). Si una regla no es válida, el servicio no inicia y el log
explica por qué, en lugar de correlacionar con reglas rotas.
