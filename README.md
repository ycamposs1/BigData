# Riesgo de superación del ECA de PM2.5 por estación — Lima Metropolitana (SENAMHI)

Proyecto integrador de Big Data y Analítica de Datos (UPAO, 2026-20). Ruta on-premise:
HDFS 3.3.6 + YARN + Spark 3.5.1 sobre Debian 12.

```
Fuentes                     bronze (HDFS, solo lectura)     silver (Parquet)              gold (Parquet)
A  SENAMHI datos abiertos ─┐                               ┌ calidad_aire_horaria
B  SENAMHI repositorio ────┼─ ingesta_bronze.sh ─► job 1 ──┼ meteo_horaria       ─► job 2 ─► features_diarias
C  NASA POWER (API) ───────┘   (+ manifiesto SHA-256)      └ aire_meteo_horaria             reporte_positivos
```

## Estructura del repositorio

```
proyecto-pm25/
├── README.md                 esta guía
├── requirements.txt          librerías de Python con versión fija
├── bin/
│   ├── ingesta_bronze.sh     copia los archivos a HDFS y registra su SHA-256
│   └── run_pipeline.sh       corrida completa con log (ingesta -> job 1 -> job 2)
├── src/
│   ├── config.py             rutas y parámetros (único lugar que se edita)
│   ├── comun.py              funciones compartidas por los jobs
│   ├── 00_inspeccionar_excel.py
│   ├── 01_xlsx_a_csv.py
│   ├── 02_descargar_nasa_power.py
│   ├── job1_silver.py        bronze -> silver: limpieza, reglas de calidad, unión
│   └── job2_gold.py          silver -> gold: promedio diario, ECA, variables del modelo
├── notebooks/                análisis exploratorio y modelo base
├── data/
│   ├── originales/           archivos tal como se obtuvieron   (NO se suben a GitHub)
│   ├── derivados/            conversiones de formato            (NO se suben a GitHub)
│   └── muestras/             recortes pequeños de datos reales  (SÍ se suben)
└── evidencias/               reportes de calidad, tiempos, manifiesto y logs de corridas
```

---

## 1. Preparar la máquina virtual (una sola vez)

**Encender HDFS y YARN** cada vez que inicias la VM:

```bash
start-dfs.sh
start-yarn.sh
jps          # deben aparecer: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```

**Agregar `HADOOP_CONF_DIR` al `.bashrc`.** spark-submit lo necesita para enviar trabajos a YARN
y la guía de instalación del curso no lo incluye:

```bash
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
source ~/.bashrc
```

**Revisar las comillas del `.bashrc`.** Si copiaste las líneas `PYSPARK_PYTHON` desde el PDF, pueden
tener comillas tipográficas (`”`) en vez de rectas (`"`), y Spark falla con
`Cannot run program "”/home/...`. Ábrelo con `nano ~/.bashrc` y reemplázalas por comillas rectas.

**Crear las zonas del data lake:**

```bash
hdfs dfs -mkdir -p /datalake/bronze /datalake/silver /datalake/gold
hdfs dfs -ls /datalake
```

Interfaces web para revisar y capturar evidencias: HDFS en `http://localhost:9870`, YARN en
`http://localhost:8088` y el job de Spark en curso en `http://localhost:4040`.

## 2. Crear la carpeta del proyecto

```bash
mkdir -p ~/BigData_UPAO/proyecto-pm25     # -p crea también las carpetas intermedias si faltan
cd ~/BigData_UPAO/proyecto-pm25            # entrar a la carpeta
pwd                                        # confirmar dónde estás
```

Copia aquí el contenido de este repositorio (por ejemplo, descomprimiendo el .zip con
`unzip ~/Descargas/proyecto-pm25.zip -d ~/BigData_UPAO/`) y revisa la estructura:

```bash
sudo apt install tree -y
tree -a -L 3 -I '.git'
chmod +x bin/*.sh                          # permiso de ejecución a los scripts
```

## 3. Subir el proyecto a GitHub

**3.1. Identificarte en Git (una vez por máquina):**

```bash
git config --global user.name  "Nombre Apellido"
git config --global user.email "correo-de-tu-cuenta-github@ejemplo.com"
```

El correo debe ser el de tu cuenta de GitHub: así cada commit cuenta a tu nombre, y en la
Semana 14 la nota es individual según tu contribución en el repositorio.

**3.2. Conectar la VM con GitHub por SSH** (GitHub ya no acepta contraseña al hacer push):

```bash
ssh-keygen -t ed25519 -C "correo-de-tu-cuenta-github@ejemplo.com"   # Enter a todo
cat ~/.ssh/id_ed25519.pub                                            # copia la línea completa
```

En GitHub: *Settings → SSH and GPG keys → New SSH key*, pega la línea y guarda. Comprueba:

```bash
ssh -T git@github.com        # debe responder: "Hi <usuario>! You've successfully authenticated"
```

**3.3. Crear el repositorio (solo el coordinador):** en GitHub, *New repository*, nombre
`proyecto-pm25`, **sin** marcar README ni .gitignore (ya existen aquí). Luego, en la VM:

```bash
cd ~/BigData_UPAO/proyecto-pm25
git init -b main
git add .
git status                   # verifica que NO aparezca ningún archivo de data/originales ni data/derivados
git commit -m "Estructura inicial del pipeline"
git remote add origin git@github.com:USUARIO/proyecto-pm25.git
git push -u origin main
```

**3.4. Sumar al equipo y al docente:** *Settings → Collaborators → Add people*. Cada integrante
clona el repositorio en su VM y trabaja con su propia cuenta:

```bash
cd ~/BigData_UPAO
git clone git@github.com:USUARIO/proyecto-pm25.git
```

**3.5. Ciclo de trabajo diario:**

```bash
git pull --rebase            # traer lo último ANTES de empezar
# ... editar, probar ...
git add src/job1_silver.py   # agregar solo lo que cambiaste
git commit -m "Job 1: lector de la fuente B"
git push
```

Los datos completos nunca se suben: el `.gitignore` excluye `data/originales/` y `data/derivados/`,
y GitHub rechaza archivos de más de 100 MB. Lo que sí se sube son las muestras (paso 8) y las
evidencias.

## 4. Entorno de Python

```bash
source ~/BigData_UPAO/bigdata_env/bin/activate
pip install -r requirements.txt
python -c "import pyspark; print(pyspark.__version__)"     # debe decir 3.5.1
```

| Librería | Para qué se usa |
| --- | --- |
| pyspark 3.5.1 | Jobs 1 y 2, Spark SQL y MLlib. Debe coincidir con el Spark de `/opt`: un `pip install pyspark` sin versión instalaría una más nueva y fallaría contra el clúster. |
| findspark | Enlaza los notebooks de Jupyter con el Spark de `/opt`. |
| numpy < 2 | Lo exige MLlib; PySpark 3.5.1 no es compatible con numpy 2. |
| pandas + openpyxl | Inspección y conversión única de los XLSX; no procesa el volumen. |
| requests | Descarga de NASA POWER. |
| matplotlib | Gráficos del análisis exploratorio. |
| jupyterlab | Notebooks de análisis exploratorio y modelo base. |

## 5. Obtener los datos (solo fuentes reales)

| Fuente | Dónde se obtiene | Carpeta local |
| --- | --- | --- |
| A. Monitoreo de contaminantes del aire en Lima Metropolitana (2015–2024) | [Plataforma Nacional de Datos Abiertos](https://www.datosabiertos.gob.pe/dataset/monitoreo-de-los-contaminantes-del-aire-en-lima-metropolitana-servicio-nacional-de), sección de recursos: descargar el CSV | `data/originales/senamhi_datosabiertos/` (solo el CSV) |
| A (documentación) | Misma página: diccionario (XLSX) y metadatos | `data/originales/docs/` |
| B. Datos horarios de contaminantes (2010–2020) | [Repositorio institucional de SENAMHI](https://repositorio.senamhi.gob.pe/handle/20.500.12542/2467): descargar el XLSX | `data/originales/senamhi_repositorio/` |
| C. Meteorología horaria | API de NASA POWER, con el script del paso 5.3 | se crea sola |

Anota en `evidencias/fuentes.md` el enlace y la **fecha de obtención** de cada archivo: son campos
obligatorios de la ficha de procedencia (sección 3.1), sin la cual el informe no se recibe.

**5.1. Revisar el formato de A antes de procesarlo:**

```bash
head -3 data/originales/senamhi_datosabiertos/*.csv     # ¿separador "," o ";"?
file -i data/originales/senamhi_datosabiertos/*.csv     # ¿charset utf-8 o iso-8859-1?
```

Si el separador o la codificación no son `,` y UTF-8, cámbialos en `src/config.py` (`A_SEP`, `A_ENCODING`).

**5.2. Inspeccionar y convertir los Excel (fuente B y cualquier otro XLSX):**

```bash
python src/00_inspeccionar_excel.py > evidencias/estructura_excel.txt
cat evidencias/estructura_excel.txt
python src/01_xlsx_a_csv.py
```

La salida de la inspección (hojas, filas y primeras filas) es lo que se necesita para escribir el
lector de B (`leer_b` en `src/job1_silver.py`). Mientras ese lector no exista, el pipeline corre
completo solo con A.

**5.3. Descargar NASA POWER:**

```bash
python src/02_descargar_nasa_power.py
```

Toma las coordenadas y la altitud de cada estación del propio CSV de A, pide la hora en UTC y envía
la altitud para que la API corrija la presión. Guarda la respuesta JSON original en
`data/originales/nasa_power_json/` y un CSV aplanado en `data/derivados/nasa_power_csv/`. Son unas
105 consultas (7 estaciones × 15 años); si se corta, se vuelve a ejecutar y continúa donde quedó.
El resumen queda en `evidencias/resumen_descarga_nasa.csv`.

## 6. Ejecutar el pipeline

```bash
bash bin/run_pipeline.sh                      # ingesta -> job 1 -> job 2 en YARN, con log
```

O paso a paso:

```bash
bash bin/ingesta_bronze.sh
spark-submit --master yarn src/job1_silver.py
spark-submit --master yarn src/job2_gold.py
```

Si YARN da problemas, depura primero en modo local con `MASTER="local[*]" bash bin/run_pipeline.sh`;
la corrida que va al informe debe ser la de YARN, porque es lo que muestra el diagrama de arquitectura.

| Paso | Qué hace |
| --- | --- |
| `ingesta_bronze.sh` | Calcula el SHA-256 de cada archivo, lo copia sin cambios a `/datalake/bronze/<carpeta>` y deja bronze en solo lectura. |
| `job1_silver.py` | A y NASA pasan de UTC a hora local de Lima. Normaliza la clave de estación, aplica las reglas R0–R7 y R10, une aire y meteorología por estación + fecha_hora y escribe silver en Parquet particionado por año. |
| `job2_gold.py` | Promedio diario de PM2.5 solo con 18 horas o más (R8), marca `supera_eca` (> 50 µg/m³), arma el objetivo `y_supera_eca_manana` y las variables del día, y genera el reporte de positivos y la línea base de persistencia. |

Nota técnica para la sección 3.8: en HDFS, el usuario que arrancó el NameNode es superusuario y no
está sujeto a permisos. El `chmod a-w` de bronze protege contra otros usuarios, no contra él; en
producción el pipeline correría con un usuario propio, sin privilegios de superusuario.

## 7. Revisar resultados y llevarlos al informe

```bash
hdfs dfs -ls -R /datalake/silver | head -30
hdfs dfs -du -h /datalake/bronze /datalake/silver /datalake/gold
ls evidencias/
```

| Archivo en `evidencias/` | Sección del informe |
| --- | --- |
| `manifiesto_bronze.csv` | 3.1 (tamaños) y 3.2 (integridad) |
| `reporte_calidad_job1.csv`, `reporte_calidad_job2.csv` | 3.4: columna «Registros afectados» de cada regla |
| `reporte_positivos.csv` | 1.4 (días que superan el ECA) y 4.1 |
| `linea_base_persistencia_prueba.csv` | 1.4, 4.2 y el ⟦Z⟧ del resumen ejecutivo |
| `tiempos.csv` | 3.6 y 3.7 |
| `logs/corrida_*.log` | Anexo: evidencia de corrida completa (requisito de admisibilidad) |

**Primera decisión con datos:** revisa en `linea_base_persistencia_prueba.csv` la columna
`positivos_prueba`. Si hay muy pocos días positivos en 2022–2024, hay que ajustar la evaluación
(ver riesgo 3 en la sección 0.3) antes de entrenar el modelo.

## 8. Subir muestras de datos reales al repositorio

La guía pide datos de muestra cuando el conjunto completo no puede publicarse:

```bash
f=$(ls data/originales/senamhi_datosabiertos/*.csv | head -1)
head -n 1001 "$f" > data/muestras/senamhi_datosabiertos_muestra.csv
f=$(ls data/derivados/nasa_power_csv/*.csv | head -1)
head -n 1001 "$f" > data/muestras/nasa_power_muestra.csv
for f in data/derivados/senamhi_repositorio_csv/*.csv; do
  head -n 1001 "$f" > "data/muestras/$(basename "$f" .csv)_muestra.csv"
done
git add data/muestras evidencias && git commit -m "Muestras y evidencias de la primera corrida" && git push
```

## 9. Pendiente para el hito de la Semana 7

1. Lector de la fuente B (`leer_b`), a partir de `evidencias/estructura_excel.txt`.
2. `notebooks/01_eda.ipynb`: al menos tres gráficos sobre gold, cada uno respondiendo una pregunta.
3. `notebooks/02_modelo_base.ipynb`: regresión logística en MLlib con partición temporal
   (entrenamiento < 2022), evaluada con PR-AUC, recall y F1 de la clase positiva, contra la persistencia.
4. Optimización medida (3.6) y prueba de escalabilidad ×10 y ×100 (3.7).

Para usar Spark desde un notebook:

```python
import os, sys, findspark
os.environ["SPARK_HOME"] = "/opt/spark-3.5.1-bin-hadoop3"
findspark.init()
sys.path.insert(0, "../src")
import config as C
from comun import crear_spark
spark = crear_spark("eda")
gold = spark.read.parquet(f"{C.GOLD}/features_diarias")
```

## 10. Problemas frecuentes

| Síntoma | Causa y solución |
| --- | --- |
| `Cannot run program "”/home/...python”"` | Comillas tipográficas en el `.bashrc` (paso 1). |
| `When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set` | Falta `HADOOP_CONF_DIR` (paso 1). |
| `Connection refused` a `localhost:9000` | HDFS no está encendido: `start-dfs.sh` y revisa con `jps`. |
| `Name node is in safe mode` | Espera un minuto tras encender; si persiste: `hdfs dfsadmin -safemode leave`. |
| HDFS "vacío" tras reiniciar la VM | Los directorios del NameNode estaban en `/tmp`; deben estar en `/opt/hadoop-3.3.6/data/` (riesgo 5). |
| Job 1 termina con "no encuentro las columnas" | Separador o codificación de A mal configurados en `src/config.py` (paso 5.1). |
| Nombres de estación con `Ã` | Cambia `A_ENCODING` a `"ISO-8859-1"`. |
| `R7` lista estaciones sin correspondencia | Los nombres de NASA y de A no producen la misma clave; revisa `silver/estaciones`. |
| `git push` rechazado por autenticación | Configura la clave SSH (paso 3.2) y usa la URL `git@github.com:...`. |
| `git push` rechazado (non-fast-forward) | Alguien subió cambios antes: `git pull --rebase` y vuelve a hacer push. |
| GitHub rechaza un archivo > 100 MB | Se agregó un dato completo por error: `git rm --cached <archivo>` y verifica el `.gitignore`. |
