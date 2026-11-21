# Detener o eliminar el entorno de Kubernetes

Kubernetes no incluye un comando `kubectl stop`. Los contenedores se ejecutan
dentro de Pods y, normalmente, un controlador vuelve a crear un Pod cuando se
elimina. Por eso, el comando correcto depende de lo que se quiera conservar.

| Objetivo | Acción recomendada | ¿Se puede reanudar? |
| --- | --- | --- |
| Liberar CPU y memoria sin perder el clúster local | Detener los nodos de `kind` | Sí |
| Detener una aplicación, pero mantener Kubernetes activo | Escalar sus `Deployment` y `StatefulSet` a cero | Sí |
| Eliminar una aplicación desplegada por AIOpsLab | Eliminar su *namespace* | Se debe desplegar de nuevo |
| Borrar completamente el clúster local | `kind delete cluster` | Se debe crear de nuevo |

> Esta guía usa el clúster local predeterminado de AIOpsLab, llamado `kind`.
> No ejecute comandos `docker stop` ni `kind delete cluster` contra un clúster
> remoto. En un clúster remoto, use el escalado descrito en la segunda opción o
> el mecanismo de apagado de su proveedor.

## 1. Comprobaciones previas

Termine primero cualquier sesión activa de AIOpsLab de forma normal. Después,
confirme qué contexto y qué clúster están seleccionados:

```bash
kubectl config current-context
kind get clusters
kubectl get nodes
```

Para la instalación local predeterminada, el contexto esperado es `kind-kind` y
el clúster mostrado por `kind get clusters` es `kind`. Si aparece otro contexto,
seleccione explícitamente el local antes de continuar:

```bash
kubectl config use-context kind-kind
```

## 2. Pausar todo el entorno local y conservarlo

Esta es la opción recomendada al terminar una jornada de trabajo. Conserva los
manifiestos y el estado de los contenedores de los nodos de `kind`.

Si Grafana o el ETL están en ejecución, deténgalos primero. Son servicios Docker
independientes del clúster:

```bash
./scripts/grafana.sh down
./scripts/etl.sh down
```

No es un error omitir el comando de un componente que no se haya desplegado.
Ambos comandos conservan sus volúmenes Docker.

El clúster predeterminado de AIOpsLab tiene dos nodos. Detenga primero el
trabajador y luego el plano de control:

```bash
docker stop kind-worker kind-control-plane
```

Compruebe que quedaron detenidos:

```bash
docker ps -a --filter name=kind-
```

### Reanudar el entorno

Inicie primero el plano de control y después el trabajador:

```bash
docker start kind-control-plane
docker start kind-worker
```

El servidor de Kubernetes puede tardar unos segundos en responder. Cuando
`kubectl cluster-info --context kind-kind` funcione, espere a que los nodos
estén listos e inspeccione los Pods:

```bash
kubectl cluster-info --context kind-kind
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl get pods --all-namespaces
```

Reinicie solamente los servicios adicionales que estuvieran en uso:

```bash
./scripts/etl.sh up
./scripts/grafana.sh up
./scripts/grafana.sh validate
```

Si se creó un clúster con otro nombre, obtenga primero los nombres reales de
sus nodos y use esos nombres con `docker stop` y `docker start`:

```bash
kind get nodes --name <nombre-del-cluster>
```

## 3. Detener solo una aplicación

Escalar a cero elimina los Pods administrados, pero conserva sus `Deployment`,
`StatefulSet` y configuración. Antes de hacerlo, anote el número actual de
réplicas para poder restaurarlo:

```bash
kubectl get deployment,statefulset --namespace <namespace>
```

Después, escale las cargas del *namespace* a cero:

```bash
kubectl scale deployment --all --replicas=0 --namespace <namespace>
kubectl scale statefulset --all --replicas=0 --namespace <namespace>
kubectl get pods --namespace <namespace>
```

Para reanudar, restaure cada carga con el número de réplicas que anotó:

```bash
kubectl scale deployment/<nombre> --replicas=<replicas-anteriores> --namespace <namespace>
kubectl scale statefulset/<nombre> --replicas=<replicas-anteriores> --namespace <namespace>
kubectl rollout status deployment/<nombre> --namespace <namespace>
```

Este método no detiene `DaemonSet`, Jobs ya iniciados ni componentes del propio
clúster. Un `HorizontalPodAutoscaler` también puede modificar otra vez el número
de réplicas. Revise esos recursos antes de usar esta opción:

```bash
kubectl get daemonset,job,cronjob,hpa --namespace <namespace>
```

No use `kubectl delete pod` como método de apagado: si el Pod pertenece a un
controlador, Kubernetes normalmente creará otro para reemplazarlo.

## 4. Eliminar una aplicación de AIOpsLab

AIOpsLab despliega cada aplicación en un *namespace*. Si ya no necesita una
ejecución, puede borrar ese *namespace* completo:

```bash
kubectl get namespaces
kubectl delete namespace <namespace>
```

Esta operación elimina las cargas y las solicitudes de volumen persistente
(`PersistentVolumeClaim`) del *namespace*. El destino de cada
`PersistentVolume` depende de la política de reclamación de su clase de
almacenamiento. No es una pausa: para volver a usar la aplicación, AIOpsLab
tendrá que desplegarla de nuevo.

## 5. Eliminar completamente el clúster local

Use esta opción únicamente cuando no necesite conservar el entorno Kubernetes:

```bash
./scripts/grafana.sh down
./scripts/etl.sh down
kind delete cluster --name kind
```

`kind delete cluster` elimina los nodos y el estado almacenado dentro del
clúster. Los volúmenes Docker de Grafana y del ETL se conservan porque sus
comandos `down` no usan la opción `-v`.

Para crear otra vez el clúster:

```bash
# x86_64
kind create cluster --name kind --config kind/kind-config-x86.yaml

# ARM64
kind create cluster --name kind --config kind/kind-config-arm.yaml
```

## Solución de problemas

- **`kubectl` muestra `connection refused` después de reiniciar:** espere unos
  segundos, confirme los nodos con `docker ps --filter name=kind-` y repita
  `kubectl cluster-info --context kind-kind`.
- **Los Pods reaparecen:** identifique su controlador con
  `kubectl get pod <pod> --namespace <namespace> -o yaml` y escale el
  `Deployment` o `StatefulSet` correspondiente.
- **Un nodo no vuelve a estar `Ready`:** consulte sus registros con
  `kind export logs --name kind` antes de considerar recrear el clúster.
- **Un Pod queda en `Terminating`:** revise eventos y registros antes de forzar
  su eliminación. `--grace-period=0 --force` no debe usarse como procedimiento
  normal de apagado.

## Referencias

- [Inicio rápido de kind](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [Escalado manual de un Deployment](https://kubernetes.io/docs/tasks/run-application/scale-deployment/)
- [Pods y controladores](https://kubernetes.io/docs/concepts/workloads/pods/)
- [Terminación de Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
