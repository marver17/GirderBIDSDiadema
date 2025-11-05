#!/usr/bin/env python3
"""
BIDS Importer per Girder
Carica dataset BIDS su istanza Girder preservando la struttura e combinando NIfTI + JSON.
"""

import argparse
import io
import json
import logging
import os
import subprocess
import sys
from enum import Enum
from urllib.parse import urlparse, urlunparse

import girder_client
import requests

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class ImportMode(Enum):
    """Modalità di importazione."""
    RESET_DATABASE = 'RESET_DATABASE'
    OVERWRITE_ON_SAME_NAME = 'OVERWRITE_ON_SAME_NAME'


def validate_bids(directory):
    """Valida un dataset BIDS usando bids-validator."""
    try:
        result = subprocess.run(
            ['bids-validator', '--json', directory],
            capture_output=True,
            text=True
        )
        output = result.stdout
        errors = result.stderr
        if errors:
            logger.error(f"Validation errors: {errors}")
            return False
        return ('"errors": []' in output or '"severity": "error"' not in output)
    except FileNotFoundError:
        logger.error("bids-validator not found. Install it first.")
        return False


def check_girder_connection(api_url):
    """Verifica se l'istanza Girder è raggiungibile."""
    try:
        parsed = urlparse(api_url)
        if not parsed.scheme:
            api_url = 'http://' + api_url
            parsed = urlparse(api_url)
        base_url = parsed.scheme + '://' + parsed.netloc
        response = requests.get(base_url, timeout=10)
        if response.status_code == 200:
            logger.info("Girder connection successful.")
            return True
        else:
            logger.error(f"Connection failed with status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return False


def delete_folder_contents(gc, folder_id):
    """Elimina tutti gli item e le sottocartelle in una cartella."""
    for item in gc.listItem(folder_id):
        try:
            gc.delete(f"/item/{item['_id']}")
        except Exception as e:
            logger.warning(f"Failed to delete item {item['_id']}: {e}")

    for folder in gc.listFolder(folder_id):
        delete_folder_contents(gc, folder["_id"])
        try:
            gc.delete(f"/folder/{folder['_id']}")
        except Exception as e:
            logger.warning(f"Failed to delete folder {folder['_id']}: {e}")


def scan_local_bids_structure(bids_root):
    """
    Scansiona la struttura BIDS locale e ritorna un dizionario con tutti i file.
    Ritorna: dict con struttura {path_relativo: {'size': int, 'type': 'nifti'|'json'|'other'}}
    """
    local_files = {}
    
    for root, dirs, files in os.walk(bids_root):
        rel_path = os.path.relpath(root, bids_root)
        if rel_path == '.':
            rel_path = ''
        
        for filename in files:
            file_path = os.path.join(root, filename)
            rel_file_path = os.path.join(rel_path, filename) if rel_path else filename
            
            file_info = {
                'size': os.path.getsize(file_path),
                'path': file_path
            }
            
            # Classifica il tipo di file
            if filename.endswith('.nii.gz') or filename.endswith('.nii'):
                file_info['type'] = 'nifti'
            elif filename.endswith('.json'):
                file_info['type'] = 'json'
            else:
                file_info['type'] = 'other'
            
            local_files[rel_file_path] = file_info
    
    return local_files


def scan_girder_structure(gc, folder_id, base_path=''):
    """
    Scansiona ricorsivamente la struttura su Girder.
    Ritorna: dict con struttura {path_relativo: {'size': int, 'item_id': str, 'files': [...]}}
    """
    girder_structure = {}
    
    # Scansiona items nella folder corrente
    for item in gc.listItem(folder_id):
        item_name = item['name']
        item_path = os.path.join(base_path, item_name) if base_path else item_name
        
        # Ottieni info sui file nell'item
        files_info = []
        total_size = 0
        for file_obj in gc.listFile(item['_id']):
            files_info.append({
                'name': file_obj['name'],
                'size': file_obj.get('size', 0),
                'file_id': file_obj['_id']
            })
            total_size += file_obj.get('size', 0)
        
        girder_structure[item_path] = {
            'item_id': item['_id'],
            'size': total_size,
            'files': files_info,
            'file_count': len(files_info)
        }
    
    # Scansiona ricorsivamente le sottocartelle
    for folder in gc.listFolder(folder_id):
        folder_name = folder['name']
        folder_path = os.path.join(base_path, folder_name) if base_path else folder_name
        
        # Ricorsione
        subfolder_structure = scan_girder_structure(gc, folder['_id'], folder_path)
        girder_structure.update(subfolder_structure)
    
    return girder_structure


def compare_structures(local_files, girder_structure):
    """
    Confronta struttura locale con Girder.
    Ritorna dizionario con: {'new': [], 'existing': [], 'modified': [], 'missing_on_local': []}
    """
    comparison = {
        'new': [],           # File presenti solo localmente
        'existing': [],      # File presenti su entrambi (stesso nome e size simile)
        'modified': [],      # File presenti su entrambi ma size diversa
        'missing_on_local': []  # File presenti solo su Girder
    }
    
    # Crea set dei path per confronto rapido
    local_paths = set(local_files.keys())
    girder_paths = set(girder_structure.keys())
    
    # File nuovi (presenti solo localmente)
    comparison['new'] = list(local_paths - girder_paths)
    
    # File che potrebbero essere già su Girder
    common_paths = local_paths & girder_paths
    
    for path in common_paths:
        local_info = local_files[path]
        girder_info = girder_structure[path]
        
        # Confronta size (tolleranza del 1% per account di compressione)
        local_size = local_info['size']
        girder_size = girder_info['size']
        size_diff = abs(local_size - girder_size)
        tolerance = max(local_size, girder_size) * 0.01
        
        if size_diff <= tolerance:
            comparison['existing'].append({
                'path': path,
                'local_size': local_size,
                'girder_size': girder_size,
                'item_id': girder_info['item_id']
            })
        else:
            comparison['modified'].append({
                'path': path,
                'local_size': local_size,
                'girder_size': girder_size,
                'item_id': girder_info['item_id']
            })
    
    # File mancanti localmente (presenti solo su Girder)
    comparison['missing_on_local'] = list(girder_paths - local_paths)
    
    return comparison


def print_comparison_report(comparison, local_files):
    """Stampa un report del confronto in formato leggibile."""
    print("\n" + "="*80)
    print("REPORT CONFRONTO LOCALE vs GIRDER")
    print("="*80)
    
    # File nuovi
    if comparison['new']:
        print(f"\n📁 FILE NUOVI (da caricare): {len(comparison['new'])}")
        for path in sorted(comparison['new'])[:20]:  # Mostra solo i primi 20
            size_mb = local_files[path]['size'] / (1024*1024)
            print(f"  + {path} ({size_mb:.2f} MB)")
        if len(comparison['new']) > 20:
            print(f"  ... e altri {len(comparison['new']) - 20} file")
    else:
        print("\n✓ Nessun file nuovo da caricare")
    
    # File esistenti
    if comparison['existing']:
        print(f"\n✓ FILE GIÀ PRESENTI (identici): {len(comparison['existing'])}")
        for item in sorted(comparison['existing'], key=lambda x: x['path'])[:10]:
            size_mb = item['local_size'] / (1024*1024)
            print(f"  = {item['path']} ({size_mb:.2f} MB)")
        if len(comparison['existing']) > 10:
            print(f"  ... e altri {len(comparison['existing']) - 10} file")
    
    # File modificati
    if comparison['modified']:
        print(f"\n⚠️  FILE MODIFICATI (size diversa): {len(comparison['modified'])}")
        for item in sorted(comparison['modified'], key=lambda x: x['path']):
            local_mb = item['local_size'] / (1024*1024)
            girder_mb = item['girder_size'] / (1024*1024)
            diff = local_mb - girder_mb
            print(f"  ≠ {item['path']}")
            print(f"     Locale: {local_mb:.2f} MB | Girder: {girder_mb:.2f} MB | Diff: {diff:+.2f} MB")
    
    # File mancanti localmente
    if comparison['missing_on_local']:
        print(f"\n⚠️  FILE SU GIRDER MA NON IN LOCALE: {len(comparison['missing_on_local'])}")
        for path in sorted(comparison['missing_on_local'])[:10]:
            print(f"  - {path}")
        if len(comparison['missing_on_local']) > 10:
            print(f"  ... e altri {len(comparison['missing_on_local']) - 10} file")
    
    # Riepilogo
    print("\n" + "="*80)
    print("RIEPILOGO:")
    print(f"  Nuovi da caricare:     {len(comparison['new'])}")
    print(f"  Già presenti:          {len(comparison['existing'])}")
    print(f"  Modificati:            {len(comparison['modified'])}")
    print(f"  Solo su Girder:        {len(comparison['missing_on_local'])}")
    print(f"  TOTALE file locali:    {len(comparison['new']) + len(comparison['existing']) + len(comparison['modified'])}")
    print("="*80 + "\n")


def check_existing_content(gc, folder_id, bids_root):
    """
    Verifica il contenuto esistente su Girder e confronta con locale.
    Ritorna il dizionario di confronto.
    """
    logger.info("Scanning local BIDS structure...")
    local_files = scan_local_bids_structure(bids_root)
    logger.info(f"Found {len(local_files)} files locally")
    
    logger.info("Scanning Girder structure...")
    girder_structure = scan_girder_structure(gc, folder_id)
    logger.info(f"Found {len(girder_structure)} items on Girder")
    
    logger.info("Comparing structures...")
    comparison = compare_structures(local_files, girder_structure)
    
    return comparison, local_files


def upload_directory_recursively(gc, local_path, girder_parent_id, parent_type='folder', skip_files=None, bids_root=None):
    """
    Upload ricorsivo di una directory su Girder, combinando NIfTI + JSON in un unico item.
    
    Args:
        skip_files: Set di percorsi relativi (dalla root BIDS) da saltare durante l'upload
        bids_root: Path assoluto alla root BIDS (per calcolare percorsi relativi)
    """
    if skip_files is None:
        skip_files = set()
    
    if bids_root is None:
        bids_root = local_path
    
    # Raccogli tutti i file nella directory corrente
    files = {}
    dirs = []
    
    for item in os.listdir(local_path):
        local_item = os.path.join(local_path, item)
        
        if os.path.isfile(local_item):
            # Calcola percorso relativo dalla BIDS root
            rel_path = os.path.relpath(local_item, bids_root)
            
            # Verifica se questo file deve essere saltato
            if rel_path in skip_files:
                logger.debug(f"Skipping existing file: {rel_path}")
                continue
            
            # Estrai nome base e estensione
            name_base = item
            if item.endswith('.nii.gz'):
                name_base = item[:-7]
            elif item.endswith('.nii'):
                name_base = item[:-4]
            elif item.endswith('.json'):
                name_base = item[:-5]
            else:
                name_base = os.path.splitext(item)[0]
            
            # Raggruppa per nome base
            if name_base not in files:
                files[name_base] = {}
            
            if item.endswith('.nii.gz') or item.endswith('.nii'):
                files[name_base]['nifti'] = local_item
            elif item.endswith('.json'):
                files[name_base]['json'] = local_item
            else:
                files[name_base]['other'] = local_item
        
        elif os.path.isdir(local_item):
            dirs.append((item, local_item))
    
    # Upload dei file
    for name_base, file_group in files.items():
        try:
            if 'nifti' in file_group and 'json' in file_group:
                # Caso BIDS: NIfTI + JSON → crea un unico item
                logger.info(f"Uploading BIDS pair: {os.path.basename(file_group['nifti'])} + {os.path.basename(file_group['json'])}")
                
                item_name = os.path.basename(file_group['nifti'])
                
                # CORREZIONE: Usa folderId per folder, parentType+parentId per collection
                if parent_type == 'folder':
                    item_data = {'folderId': girder_parent_id, 'name': item_name}
                else:  # collection
                    item_data = {'parentType': parent_type, 'parentId': girder_parent_id, 'name': item_name}
                
                item = gc.post('item', data=item_data)
                
                # Upload NIfTI file nell'item
                gc.uploadFileToItem(item['_id'], file_group['nifti'])
                
                # Upload JSON file nello stesso item
                gc.uploadFileToItem(item['_id'], file_group['json'])
                
                logger.info(f"  ✓ Created item '{item_name}' with NIfTI + JSON")
                
            elif 'nifti' in file_group:
                # Solo NIfTI senza JSON
                logger.info(f"Uploading NIfTI (no JSON): {os.path.basename(file_group['nifti'])}")
                gc.upload(file_group['nifti'], girder_parent_id, parentType=parent_type)
                
            elif 'json' in file_group:
                # Solo JSON senza NIfTI (es. dataset_description.json)
                logger.info(f"Uploading JSON: {os.path.basename(file_group['json'])}")
                gc.upload(file_group['json'], girder_parent_id, parentType=parent_type)
                
            elif 'other' in file_group:
                # Altri file (tsv, txt, ecc.)
                logger.info(f"Uploading file: {os.path.basename(file_group['other'])}")
                gc.upload(file_group['other'], girder_parent_id, parentType=parent_type)
                
        except Exception as e:
            logger.warning(f"Failed to upload {name_base}: {e}")
    
    # Upload ricorsivo delle sottocartelle
    for dir_name, dir_path in dirs:
        try:
            # Prova a creare la folder
            folder_data = {'parentType': parent_type, 'parentId': girder_parent_id, 'name': dir_name}
            new_folder = gc.post('folder', data=folder_data)
            logger.info(f"Created folder: {dir_name}")
            folder_id = new_folder['_id']
            
        except Exception as e:
            # Se la folder esiste già, recuperala
            if "already exists" in str(e).lower():
                logger.debug(f"Folder {dir_name} already exists, using existing folder")
                # Cerca la folder esistente
                existing_folders = list(gc.listFolder(girder_parent_id, parentFolderType=parent_type))
                matching_folder = next((f for f in existing_folders if f['name'] == dir_name), None)
                
                if matching_folder:
                    folder_id = matching_folder['_id']
                else:
                    logger.warning(f"Could not find or create folder {dir_name}")
                    continue
            else:
                logger.warning(f"Failed to create folder {dir_name}: {e}")
                continue
        
        # Ricorsione con propagazione skip_files e bids_root
        try:
            upload_directory_recursively(gc, dir_path, folder_id, 'folder', skip_files, bids_root)
        except Exception as e:
            logger.warning(f"Failed to upload contents of {dir_name}: {e}")


def get_file_metadata(f):
    """Legge un file JSON e restituisce un dict."""
    f.seek(0, 0)
    return json.load(f)


def is_bids_item(item):
    """Verifica se un item è un file JSON BIDS."""
    return item['name'].endswith('.json')


def get_associated_id(gc, parent_id, bids_item):
    """Trova l'ID associato a un file JSON BIDS."""
    file_name = bids_item['name']
    if file_name == 'dataset_description.json':
        return parent_id, 'folder'
    file_base, _ = os.path.splitext(file_name)
    for item in gc.listItem(parent_id):
        if item['name'].startswith(file_base):
            return item['_id'], 'item'
    return (None, None)


def extract_bids_metadata(gc, folder_id, recursive=True):
    """Estrae metadati da file JSON BIDS e li assegna agli item in Girder."""
    for item in gc.listItem(folder_id):
        if is_bids_item(item):
            associated_id, assoc_type = get_associated_id(gc, folder_id, item)
            if associated_id is None:
                logger.debug(f"No associated resource for {item['name']}")
                continue
            bids_file = next(gc.listFile(item['_id'], limit=1))
            file_obj = io.BytesIO()
            for chunk in gc.downloadFileAsIterator(bids_file['_id']):
                if chunk:
                    file_obj.write(chunk)
            metadata = get_file_metadata(file_obj)
            try:
                if assoc_type == 'item':
                    gc.addMetadataToItem(associated_id, metadata)
                elif assoc_type == 'folder':
                    gc.addMetadataToFolder(associated_id, metadata)
            except Exception as e:
                logger.warning(f"Failed to add metadata: {e}")

    if recursive:
        for child_folder in gc.listFolder(folder_id):
            extract_bids_metadata(gc, child_folder['_id'])


def upload_to_girder(api_url, api_key, root_folder_id, bids_root, import_mode, skip_files=None):
    """
    Carica i file BIDS su Girder preservando la gerarchia.
    
    Args:
        skip_files: Set di percorsi relativi da saltare durante l'upload
    """
    if skip_files is None:
        skip_files = set()
    
    parsed = urlparse(api_url)
    if not parsed.scheme:
        api_url = 'http://' + api_url
        parsed = urlparse(api_url)

    base_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))

    if not check_girder_connection(base_url):
        logger.error("Cannot connect to Girder. Aborting.")
        return False

    try:
        gc = girder_client.GirderClient(apiUrl=api_url)
        gc.authenticate(apiKey=api_key)
    except Exception as e:
        logger.error(f"Failed to authenticate: {e}")
        return False

    # Verifica che il folder esista
    try:
        folder_info = gc.getFolder(root_folder_id)
        logger.info(f"Target folder found: {folder_info['name']} ({root_folder_id})")
    except Exception as e:
        logger.error(f"Target folder not found: {e}")
        return False

    if import_mode == ImportMode.RESET_DATABASE:
        logger.info(f"Deleting folder contents {root_folder_id}")
        delete_folder_contents(gc, root_folder_id)

    try:
        logger.info(f"Uploading BIDS dataset from {bids_root} to folder {root_folder_id}")
        if skip_files:
            logger.info(f"Skipping {len(skip_files)} existing files")
        upload_directory_recursively(gc, bids_root, root_folder_id, 'folder', skip_files, bids_root)
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return False

    try:
        logger.info("Extracting BIDS metadata...")
        extract_bids_metadata(gc, root_folder_id)
    except Exception as e:
        logger.warning(f"extract_bids_metadata failed: {e}")

    logger.info("Upload complete!")
    return True


def main():
    """Funzione principale con argparse per CLI."""
    parser = argparse.ArgumentParser(
        description='BIDS Importer per Girder - Carica dataset BIDS preservando la struttura',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  %(prog)s --bids-dir /path/to/bids --api-url http://localhost:8081/api/v1 \\
           --api-key YOUR_KEY --folder-id FOLDER_ID

  %(prog)s --bids-dir /data/bids --api-url localhost:8081/api/v1 \\
           --api-key abc123 --folder-id 123456 --reset --no-validate
        """
    )
    
    # Argomenti richiesti
    parser.add_argument(
        '--bids-dir',
        required=True,
        help='Percorso alla directory BIDS da caricare'
    )
    parser.add_argument(
        '--api-url',
        required=True,
        help='URL API di Girder (es: http://localhost:8081/api/v1)'
    )
    parser.add_argument(
        '--api-key',
        required=True,
        help='Chiave API di Girder per autenticazione'
    )
    parser.add_argument(
        '--folder-id',
        required=True,
        help='ID del folder Girder di destinazione'
    )
    
    # Argomenti opzionali
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Elimina il contenuto del folder prima dell\'upload (RESET_DATABASE)'
    )
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Salta la validazione BIDS prima dell\'upload'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Output verboso con logging DEBUG'
    )
    parser.add_argument(
        '--compare',
        action='store_true',
        help='Confronta i file locali con il contenuto su Girder e mostra le differenze (non carica)'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Salta il caricamento dei file già presenti su Girder'
    )
    
    args = parser.parse_args()
    
    # Imposta livello logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determina import mode
    import_mode = ImportMode.RESET_DATABASE if args.reset else ImportMode.OVERWRITE_ON_SAME_NAME
    
    # Validazione BIDS (opzionale)
    if not args.no_validate:
        logger.info("Validating BIDS dataset...")
        if validate_bids(args.bids_dir):
            logger.info("BIDS dataset is valid ✓")
        else:
            logger.error("BIDS validation failed. Use --no-validate to skip.")
            return 1
    else:
        logger.info("Skipping BIDS validation (--no-validate)")
    
    # Modalità compare: solo confronto senza upload
    if args.compare:
        logger.info("Compare mode: checking existing content...")
        
        # Connessione a Girder
        try:
            from urllib.parse import urlparse, urlunparse
            
            parsed = urlparse(args.api_url)
            if not parsed.scheme:
                parsed = urlparse(f"http://{args.api_url}")
            
            base_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
            
            if not check_girder_connection(base_url):
                logger.error("Cannot connect to Girder. Aborting.")
                return 1
            
            gc = girder_client.GirderClient(apiUrl=args.api_url)
            gc.authenticate(apiKey=args.api_key)
            
            # Verifica folder
            folder_info = gc.getFolder(args.folder_id)
            logger.info(f"Target folder: {folder_info['name']} ({args.folder_id})")
            
            # Esegui confronto
            comparison, local_files = check_existing_content(gc, args.folder_id, args.bids_dir)
            print_comparison_report(comparison, local_files)
            
            return 0
            
        except Exception as e:
            logger.error(f"Compare failed: {e}")
            import traceback
            traceback.print_exc()
            return 1
    
    # Upload normale o con skip-existing
    logger.info("Starting upload to Girder...")
    
    skip_files = set()
    
    # Se --skip-existing, calcola la lista dei file da escludere
    if args.skip_existing:
        try:
            from urllib.parse import urlparse, urlunparse
            
            parsed = urlparse(args.api_url)
            if not parsed.scheme:
                parsed = urlparse(f"http://{args.api_url}")
            
            base_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
            
            if not check_girder_connection(base_url):
                logger.error("Cannot connect to Girder. Aborting.")
                return 1
            
            gc = girder_client.GirderClient(apiUrl=args.api_url)
            gc.authenticate(apiKey=args.api_key)
            
            logger.info("Checking existing content to skip...")
            comparison, local_files = check_existing_content(gc, args.folder_id, args.bids_dir)
            
            # Crea set dei file da saltare (quelli già presenti)
            for existing_item in comparison['existing']:
                skip_files.add(existing_item['path'])
            
            # Mostra statistiche
            logger.info(f"Files to skip (existing): {len(skip_files)}")
            logger.info(f"Files to upload (new): {len(comparison['new'])}")
            logger.info(f"Files to upload (modified): {len(comparison['modified'])}")
            logger.info(f"Total files to upload: {len(comparison['new']) + len(comparison['modified'])}")
            
        except Exception as e:
            logger.error(f"Could not check existing content: {e}")
            logger.error("Aborting upload due to --skip-existing check failure")
            return 1
    
    success = upload_to_girder(
        args.api_url,
        args.api_key,
        args.folder_id,
        args.bids_dir,
        import_mode,
        skip_files
    )
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
