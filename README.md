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
    [--verbose] \
    [--no-ssl-verify | --certificate /path/to/cert.pem]
```

### Flag disponibili:
- `--compare`: Confronta locale vs Girder senza caricare (dry-run)
- `--skip-existing`: Salta i file già presenti (upload incrementale)
- `--reset`: Elimina tutto prima di caricare (ricomincia da zero)
- `--no-validate`: Salta la validazione BIDS
- `--verbose`: Output dettagliato (livello DEBUG)
- `--quiet`: Mostra solo warning ed errori (riduce output)
- `--no-progress`: Disabilita le progress bar durante l'upload
- `--no-ssl-verify`: Disabilita verifica certificato SSL (utile per certificati self-signed)
- `--certificate /path/to/cert.pem`: Usa un certificato CA personalizzato

### Funzionalità principali:
- **Combinazione NIfTI + JSON**: File `.nii.gz` e `.json` con stesso nome vengono combinati in un unico item
- **Confronto intelligente**: Identifica file nuovi, esistenti e modificati
- **Upload incrementale**: Con `--skip-existing` carica solo ciò che manca
- **Gestione folder esistenti**: Riutilizza folder già create invece di generare errori
- **Supporto SSL/TLS**: Gestione certificati self-signed e CA personalizzati

### Gestione Certificati SSL

#### Caso 1: Server con certificato self-signed
Se il server Girder usa un certificato self-signed, puoi disabilitare la verifica SSL:
```bash
python3 tools/bids-importer.py \
    --bids-dir /mnt/diadema/BIDSsample \
    --api-url https://girder.com \
    --api-key YOUR_API_KEY \
    --folder-id FOLDER_ID \
    --no-ssl-verify \
    --no-validate
```

#### Caso 2: Server con certificato CA personalizzato
Per una connessione più sicura, usa un certificato CA personalizzato:
```bash
python3 tools/bids-importer.py \
    --bids-dir /mnt/diadema/BIDSsample \
    --api-url https://girder.com \
    --api-key YOUR_API_KEY \
    --folder-id FOLDER_ID \
    --certificate /path/to/ca-cert.pem \
    --no-validate
```

## Funzionalità Implementate

- ✅ SSL/TLS support con certificati self-signed e CA personalizzati
- ✅ Upload metadati JSON prima dei file NIfTI
- ✅ Fix creazione item duplicati con --skip-existing
- ✅ Flag --quiet per ridurre output (solo WARNING/ERROR)
- ✅ Progress bar con tqdm per monitorare upload
- ✅ Flag --no-progress per disabilitare progress bar
- ✅ Soppressione automatica warning SSL urllib3 con --no-ssl-verify
