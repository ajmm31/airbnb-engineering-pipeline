# 🚀 Guía para Subir el Proyecto a GitHub

## Método 1: Desde Databricks Repos (Recomendado)

### Paso 1: Crear Repositorio en GitHub
1. Ve a https://github.com/new
2. Nombre del repositorio: `airbnb-engineering-pipeline`
3. Descripción: "Pipeline Medallion de datos de Airbnb en Databricks"
4. Visibilidad: Público o Privado (tu elección)
5. ❌ **NO marques** "Initialize with README" (ya tienes uno)
6. Click **"Create repository"**

### Paso 2: Conectar Repo en Databricks
1. En Databricks, ve a **Repos** (icono de Git en la barra lateral)
2. Click **"Add Repo"**
3. En "Git repository URL", pega: `https://github.com/<TU-USUARIO>/airbnb-engineering-pipeline`
4. Click **"Create Repo"**

### Paso 3: Copiar Archivos al Repo Conectado
Ejecuta este código en un notebook de Databricks:

```python
import shutil
import os

# REEMPLAZA esto con tu usuario de GitHub
tu_usuario_github = "TU-USUARIO-AQUI"

# Rutas
origen = "/Workspace/Repos/ajmm31@gmail.com/airbnb-engineering-pipeline"
destino = f"/Workspace/Repos/{tu_usuario_github}/airbnb-engineering-pipeline"

# Crear directorios
os.makedirs(f"{destino}/notebooks", exist_ok=True)
os.makedirs(f"{destino}/dashboards", exist_ok=True)

# Copiar notebooks
for file in os.listdir(f"{origen}/notebooks"):
    if file.endswith('.ipynb'):
        shutil.copy2(f"{origen}/notebooks/{file}", f"{destino}/notebooks/{file}")
        print(f"✅ Copiado: {file}")

# Copiar dashboards
for file in os.listdir(f"{origen}/dashboards"):
    shutil.copy2(f"{origen}/dashboards/{file}", f"{destino}/dashboards/{file}")
    print(f"✅ Copiado: dashboards/{file}")

# Copiar README
shutil.copy2(f"{origen}/README.md", f"{destino}/README.md")
print(f"✅ Copiado: README.md")

print("\n🎉 ¡Archivos copiados exitosamente!")
```

### Paso 4: Hacer Commit y Push
1. En Databricks, navega al repo recién creado
2. En la parte superior, verás cambios sin commit
3. Click en **"Commit & Push"**
4. Escribe un mensaje: "Initial commit: Pipeline Medallion completo con dashboard"
5. Click **"Commit & Push"**

¡Listo! Tu proyecto estará en GitHub.

---

## Método 2: Desde tu Computadora Local

### Paso 1: Descargar Archivos de Databricks

**Opción A: Usar Databricks CLI**
```bash
# Instalar CLI
pip install databricks-cli

# Configurar
databricks configure --token
# Host: https://<tu-workspace>.cloud.databricks.com
# Token: Genera uno en User Settings > Access Tokens

# Exportar archivos
databricks workspace export_dir \
  /Repos/ajmm31@gmail.com/airbnb-engineering-pipeline \
  ./airbnb-engineering-pipeline \
  --format SOURCE
```

**Opción B: Descargar Manualmente**
1. En Databricks, navega a cada notebook
2. Click **File** → **Export** → **IPython Notebook**
3. Guarda en tu computadora en la estructura correcta

**Opción C: Copiar Contenido**
He creado todos los archivos. Puedes:
1. Crear los archivos localmente
2. Copiar el contenido de cada README, notebook, etc.

### Paso 2: Crear Repositorio Local
```bash
# Crear directorio
mkdir airbnb-engineering-pipeline
cd airbnb-engineering-pipeline

# Inicializar Git
git init

# Crear estructura
mkdir notebooks dashboards

# Copiar tus archivos descargados aquí
```

### Paso 3: Crear .gitignore
```bash
cat > .gitignore << 'GITIGNORE'
# Databricks
.databricks/
*.pyc
__pycache__/

# Jupyter/IPython
.ipynb_checkpoints/
*.ipynb_checkpoints

# Data files (si incluyes datos, descomenta)
# *.csv
# data/

# OS files
.DS_Store
Thumbs.db

# Python
venv/
env/
*.egg-info/
GITIGNORE
```

### Paso 4: Subir a GitHub
```bash
# Agregar archivos
git add .

# Commit
git commit -m "Initial commit: Pipeline Medallion completo con dashboard"

# Conectar con GitHub (reemplaza <tu-usuario>)
git remote add origin https://github.com/<tu-usuario>/airbnb-engineering-pipeline.git

# Renombrar rama a main (si es master)
git branch -M main

# Push
git push -u origin main
```

---

## Método 3: Usar API de Databricks

```python
import requests
import base64
import os

# Configuración
DATABRICKS_HOST = "https://<workspace>.cloud.databricks.com"
DATABRICKS_TOKEN = "<tu-token>"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
}

def export_notebook(notebook_path, output_path):
    """Exportar notebook de Databricks"""
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/export"
    params = {
        "path": notebook_path,
        "format": "SOURCE"
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        content = base64.b64decode(response.json()["content"])
        with open(output_path, "wb") as f:
            f.write(content)
        print(f"✅ Exportado: {output_path}")
    else:
        print(f"❌ Error: {response.text}")

# Exportar notebooks
notebooks = [
    "/Repos/ajmm31@gmail.com/airbnb-engineering-pipeline/notebooks/01_bronze",
    "/Repos/ajmm31@gmail.com/airbnb-engineering-pipeline/notebooks/02_silver",
    "/Repos/ajmm31@gmail.com/airbnb-engineering-pipeline/notebooks/03_gold"
]

for nb in notebooks:
    name = nb.split("/")[-1]
    export_notebook(nb, f"./notebooks/{name}.ipynb")
```

---

## Verificación Final

Después de subir a GitHub, verifica que tienes:

```
✅ README.md principal con documentación completa
✅ notebooks/01_bronze.ipynb
✅ notebooks/02_silver.ipynb
✅ notebooks/03_gold.ipynb
✅ dashboards/airbnb_executive_analytics.lvdash.json
✅ dashboards/README.md con instrucciones de importación
✅ .gitignore (opcional pero recomendado)
✅ LICENSE (opcional - MIT recomendada)
```

## Siguiente Paso: Personalizar README

Edita el README.md principal y actualiza:
- [ ] Tu nombre en la sección "Autor"
- [ ] Tu usuario de GitHub
- [ ] Tu perfil de LinkedIn
- [ ] Capturas de pantalla del dashboard (opcional)
- [ ] Badge de licencia

## Tips Adicionales

### Agregar Screenshots
1. Toma capturas del dashboard
2. Sube a un directorio `images/` en el repo
3. Agrégalas al README:
```markdown
![Dashboard Overview](images/dashboard-overview.png)
```

### Agregar Badges
Agrega badges al README para dar profesionalismo:
```markdown
![Databricks](https://img.shields.io/badge/Databricks-Platform-red)
![Python](https://img.shields.io/badge/Python-3.8+-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.0+-orange)
![License](https://img.shields.io/badge/License-MIT-green)
```

### Crear LICENSE
```bash
# MIT License (recomendada para proyectos educacionales)
cat > LICENSE << 'MIT'
MIT License

Copyright (c) 2026 [Tu Nombre]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

[Texto completo de MIT License]
MIT
```

---

## ❓ Troubleshooting

**"Permission denied" al hacer push**
- Verifica tus credenciales de GitHub
- Usa Personal Access Token en lugar de contraseña
- Genera token en: GitHub Settings → Developer Settings → Personal Access Tokens

**"Remote already exists"**
```bash
git remote remove origin
git remote add origin <url-correcta>
```

**Conflictos al hacer push**
```bash
git pull origin main --rebase
git push origin main
```

---

¡Tu proyecto estará disponible en GitHub y cualquiera podrá clonarlo e implementarlo en su propio workspace de Databricks! 🎉
