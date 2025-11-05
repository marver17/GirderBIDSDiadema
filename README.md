# GirderBIDS

Girder plugin to import a BIDS database


## 1. Import BIDS database

**IMPORTANTE**: Attiva l'environment conda prima di eseguire i comandi:
```bash
conda activate girderbids
```

### Opzione 1: Confronta contenuto (senza upload)
```bash
./compare-content.sh
```
Mostra quali file sono già presenti su Girder e quali sono nuovi.

### Opzione 2: Carica solo file nuovi (skip existing)
```bash
./run-importer-skip-existing.sh
```
Carica solo i file che non sono già presenti su Girder (consigliato per aggiornamenti incrementali).

### Opzione 3: Carica tutti i file
```bash
./run-importer.sh
```
Carica tutti i file, sovrascrivendo quelli esistenti.

### Opzione 4: Comando manuale completo
```bash
python3 tools/bids-importer.py \
    --bids-dir /path/to/bids \
    --api-url http://localhost:8081/api/v1 \
    --api-key YOUR_API_KEY \
    --folder-id FOLDER_ID \
    [--compare | --skip-existing | --reset] \
    [--no-validate] \
    [--verbose]
```

### Flag disponibili:
- `--compare`: Confronta locale vs Girder senza caricare (dry-run)
- `--skip-existing`: Salta i file già presenti (upload incrementale)
- `--reset`: Elimina tutto prima di caricare (ricomincia da zero)
- `--no-validate`: Salta la validazione BIDS
- `--verbose`: Output dettagliato

### Funzionalità principali:
- **Combinazione NIfTI + JSON**: File `.nii.gz` e `.json` con stesso nome vengono combinati in un unico item
- **Confronto intelligente**: Identifica file nuovi, esistenti e modificati
- **Upload incrementale**: Con `--skip-existing` carica solo ciò che manca
- **Gestione folder esistenti**: Riutilizza folder già create invece di generare errori

### Esempio output confronto:
```
📁 FILE NUOVI (da caricare): 18025
✓ FILE GIÀ PRESENTI (identici): 827
⚠️  FILE MODIFICATI (size diversa): 1
TOTALE file locali: 18853
```
