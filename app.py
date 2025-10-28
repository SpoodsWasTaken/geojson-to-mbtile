import os
import shutil
import subprocess
import tempfile
import zipfile
import requests
import json
import sqlite3
from pathlib import Path
from collections import defaultdict
from flask import Flask, request, render_template, send_file, jsonify, session, redirect, url_for
from mapbox import Uploader
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configuration from environment variables
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size
ALLOWED_EXTENSIONS = {'zip'}

# Authentication
APP_PASSCODE = os.environ.get('PASSCODE', 'changeme')

# Default tileset configuration
DEFAULT_STAGING_TILESET = os.environ.get('DEFAULT_STAGING_TILESET', '')
DEFAULT_PRODUCTION_TILESET = os.environ.get('DEFAULT_PRODUCTION_TILESET', '')
MAPBOX_SECRET_TOKEN = os.environ.get('MAPBOX_SECRET_TOKEN', '')
MAPBOX_PUBLIC_TOKEN = os.environ.get('MAPBOX_PUBLIC_TOKEN', '')

# Data storage directory
DATA_DIR = Path('/tmp/tileset_data')
DATA_DIR.mkdir(exist_ok=True)

# MBTiles storage directory (use Railway volume if available)
MBTILES_STORAGE_DIR = Path(os.environ.get('MBTILES_STORAGE_PATH', '/data/mbtiles'))
MBTILES_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

def require_auth(f):
    """Decorator to require authentication for protected routes."""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return jsonify({'error': 'Authentication required', 'authenticated': False}), 401
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_mbtiles_layers(mbtiles_path):
    """Extract list of layer names from an MBTiles file."""
    try:
        conn = sqlite3.connect(mbtiles_path)
        cursor = conn.cursor()
        
        # Get metadata JSON which contains layer information
        cursor.execute("SELECT value FROM metadata WHERE name='json'")
        result = cursor.fetchone()
        conn.close()
        
        if result:
            metadata = json.loads(result[0])
            layers = [layer['id'] for layer in metadata.get('vector_layers', [])]
            return layers
        return []
    except Exception as e:
        print(f"Error reading MBTiles layers: {e}")
        return []

def feature_level_deduplicate(existing_mbtiles_path, new_geojson_files, new_airports, work_dir, output_path):
    """
    Perform feature-level de-duplication by airport_id.
    
    Args:
        existing_mbtiles_path: Path to existing MBTiles file
        new_geojson_files: List of paths to new GeoJSON files
        new_airports: Set of airport_ids in new upload
        work_dir: Working directory for temporary files
        output_path: Path for output MBTiles
    
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"🔍 Starting feature-level de-duplication for airports: {new_airports}")
        
        # Step 1: Decode existing MBTiles to GeoJSON by layer
        existing_geojson_dir = os.path.join(work_dir, 'existing_geojson')
        os.makedirs(existing_geojson_dir, exist_ok=True)
        
        # Get layers from existing tileset
        existing_layers = get_mbtiles_layers(existing_mbtiles_path)
        print(f"📋 Existing layers: {existing_layers}")
        
        # Decode each layer to GeoJSON
        filtered_geojson_files = []
        for layer in existing_layers:
            layer_geojson = os.path.join(existing_geojson_dir, f"{layer}.geojson")
            
            # Use tippecanoe-decode to extract layer
            decode_cmd = [
                'tippecanoe-decode',
                '-l', layer,
                existing_mbtiles_path
            ]
            
            with open(layer_geojson, 'w') as f:
                result = subprocess.run(
                    decode_cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    text=True
                )
            
            if result.returncode != 0:
                print(f"⚠️  Warning: Failed to decode layer {layer}: {result.stderr}")
                continue
            
            # Step 2: Filter out features with matching airport_ids
            with open(layer_geojson, 'r') as f:
                data = json.load(f)
            
            original_count = len(data.get('features', []))
            
            # Keep only features that DON'T match new airports
            filtered_features = [
                feature for feature in data.get('features', [])
                if feature.get('properties', {}).get('airport_id') not in new_airports
            ]
            
            filtered_count = len(filtered_features)
            removed_count = original_count - filtered_count
            
            if removed_count > 0:
                print(f"  Layer {layer}: Removed {removed_count} features from {list(new_airports)}")
            
            # Save filtered GeoJSON
            if filtered_features:
                data['features'] = filtered_features
                filtered_layer_path = os.path.join(work_dir, f"filtered_{layer}.geojson")
                with open(filtered_layer_path, 'w') as f:
                    json.dump(data, f)
                filtered_geojson_files.append(filtered_layer_path)
        
        # Step 3: Combine filtered existing + new GeoJSON files
        all_geojson_files = filtered_geojson_files + new_geojson_files
        
        if not all_geojson_files:
            print("⚠️  No GeoJSON files to process")
            return False
        
        print(f"📦 Creating MBTiles from {len(all_geojson_files)} GeoJSON files")
        
        # Step 4: Create MBTiles from combined GeoJSON
        tippecanoe_cmd = [
            'tippecanoe',
            '-o', output_path,
            '--force',
            '--no-tile-compression',
            '-Z0',
            '-z14',
            '--drop-densest-as-needed',
            '--extend-zooms-if-still-dropping'
        ]
        
        # Add all GeoJSON files
        tippecanoe_cmd.extend(all_geojson_files)
        
        result = subprocess.run(
            tippecanoe_cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Tippecanoe failed: {result.stderr}")
            return False
        
        print(f"✅ Feature-level de-duplication complete")
        return True
        
    except Exception as e:
        print(f"❌ Feature-level de-duplication failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_airports_from_tileset(tileset_id, access_token):
    """Extract unique airport codes from a Mapbox tileset."""
    try:
        # Query tileset metadata
        url = f"https://api.mapbox.com/v4/{tileset_id}.json?secure&access_token={access_token}"
        response = requests.get(url)
        
        if response.status_code != 200:
            return []
        
        data = response.json()
        
        # For now, return empty list as we need to query actual features
        # This would require querying tiles which is complex
        # Better approach: extract from MBTiles file during upload
        return []
    except Exception as e:
        print(f"Error getting airports from tileset: {e}")
        return []

@app.route('/')
def index():
    """Render the upload form."""
    return render_template('index.html', 
                         default_staging_tileset=DEFAULT_STAGING_TILESET,
                         default_production_tileset=DEFAULT_PRODUCTION_TILESET,
                         mapbox_secret_token=MAPBOX_SECRET_TOKEN,
                         mapbox_public_token=MAPBOX_PUBLIC_TOKEN)

@app.route('/auth/check')
def check_auth():
    """Check if user is authenticated."""
    return jsonify({'authenticated': session.get('authenticated', False)})

@app.route('/auth/login', methods=['POST'])
def login():
    """Authenticate user with passcode."""
    data = request.get_json()
    passcode = data.get('passcode', '')
    
    if passcode == APP_PASSCODE:
        session['authenticated'] = True
        session.permanent = True  # Session lasts 24 hours
        return jsonify({'success': True, 'message': 'Authentication successful'})
    else:
        return jsonify({'success': False, 'message': 'Invalid passcode'}), 401

@app.route('/auth/logout', methods=['POST'])
def logout():
    """Logout user."""
    session.pop('authenticated', None)
    return jsonify({'success': True, 'message': 'Logged out successfully'})

@app.route('/viewer')
@require_auth
def viewer():
    """Render the map viewer (requires authentication)."""
    tileset_id = request.args.get('tileset_id', DEFAULT_STAGING_TILESET or 'ericbutton.staging-9r4spd')
    return render_template('viewer.html', 
                         tileset_id=tileset_id,
                         mapbox_public_token=MAPBOX_PUBLIC_TOKEN)

@app.route('/health')
def health():
    """Health check endpoint for Railway."""
    return jsonify({"status": "healthy"}), 200

@app.route('/api/airports/<tileset_id>')
@require_auth
def get_airports(tileset_id):
    """Get airports list for a specific tileset."""
    # Sanitize tileset_id for filename
    airports_filename = f"{tileset_id.replace('.', '_').replace('/', '_')}_airports.json"
    airports_filepath = DATA_DIR / airports_filename
    
    if not airports_filepath.exists():
        return jsonify({
            "error": "Airports data not found for this tileset",
            "tileset_id": tileset_id
        }), 404
    
    try:
        with open(airports_filepath, 'r') as f:
            airports = json.load(f)
        return jsonify({
            "success": True,
            "tileset_id": tileset_id,
            "airports": airports,
            "count": len(airports)
        })
    except Exception as e:
        return jsonify({
            "error": "Failed to load airports data",
            "details": str(e)
        }), 500

@app.route('/upload', methods=['POST'])
def upload():
    """
    Handle file upload, convert GeoJSON files to MBTiles, and optionally upload to Mapbox.
    """
    # Validate file upload
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type. Please upload a .zip file"}), 400

    # Get output mode and related parameters
    output_mode = request.form.get('output_mode', 'download').strip()
    
    # Mapbox-specific parameters
    tileset_id = request.form.get('tileset_id', '').strip()
    update_mode = request.form.get('update_mode', 'append').strip()
    mapbox_token = request.form.get('mapbox_token', '').strip()
    
    print(f"🔍 Upload mode: {update_mode}, Tileset: {tileset_id}")
    
    # Validate Mapbox parameters if needed
    if output_mode == 'mapbox':
        # Require authentication for Mapbox operations
        if not session.get('authenticated'):
            return jsonify({"error": "Authentication required for Mapbox operations"}), 401
        
        if not mapbox_token:
            return jsonify({"error": "Mapbox access token is required for Mapbox upload"}), 400
        if not tileset_id:
            return jsonify({"error": "Tileset ID is required for Mapbox upload"}), 400
        if '.' not in tileset_id:
            return jsonify({"error": "Tileset ID must be in format: username.tileset-name"}), 400

    # Create a temporary directory for processing
    with tempfile.TemporaryDirectory() as work_dir:
        try:
            # Set up paths
            input_zip_path = os.path.join(work_dir, 'input.zip')
            geojson_dir = os.path.join(work_dir, 'geojson_files')
            temp_mbtiles_dir = os.path.join(work_dir, 'temp_mbtiles')
            output_mbtiles_path = os.path.join(work_dir, 'output.mbtiles')

            os.makedirs(geojson_dir)
            os.makedirs(temp_mbtiles_dir)

            # Save uploaded file
            file.save(input_zip_path)

            # Step 1: Unzip the file
            try:
                with zipfile.ZipFile(input_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(geojson_dir)
            except zipfile.BadZipFile:
                return jsonify({"error": "Invalid or corrupted ZIP file"}), 400

            # Find all GeoJSON files
            geojson_files = list(Path(geojson_dir).rglob('*.geojson'))
            if not geojson_files:
                return jsonify({
                    "error": "No .geojson files found in the ZIP archive"
                }), 400

            # Step 2: Preprocess GeoJSON files to add airport_id property
            # Also collect airport information for metadata
            airports_data = {}  # {airport_code: {"bounds": [minLon, minLat, maxLon, maxLat], "count": N}}
            
            for geojson_file in geojson_files:
                try:
                    with open(geojson_file, 'r') as f:
                        data = json.load(f)
                    
                    # Extract airport code from filename (part before dash/underscore)
                    filename = geojson_file.stem
                    if '-' in filename or '_' in filename:
                        normalized = filename.replace('_', '-')
                        airport_code = normalized.split('-')[0].upper()
                        
                        # Initialize airport data if not exists
                        if airport_code not in airports_data:
                            airports_data[airport_code] = {
                                "bounds": [float('inf'), float('inf'), float('-inf'), float('-inf')],
                                "count": 0
                            }
                        
                        # Add airport_id to each feature and update bounds
                        if 'features' in data:
                            for feature in data['features']:
                                if 'properties' not in feature:
                                    feature['properties'] = {}
                                feature['properties']['airport_id'] = airport_code
                                airports_data[airport_code]['count'] += 1
                                
                                # Update bounds from feature geometry
                                geom = feature.get('geometry', {})
                                if geom.get('type') == 'Point':
                                    coords = geom['coordinates']
                                    airports_data[airport_code]['bounds'][0] = min(airports_data[airport_code]['bounds'][0], coords[0])
                                    airports_data[airport_code]['bounds'][1] = min(airports_data[airport_code]['bounds'][1], coords[1])
                                    airports_data[airport_code]['bounds'][2] = max(airports_data[airport_code]['bounds'][2], coords[0])
                                    airports_data[airport_code]['bounds'][3] = max(airports_data[airport_code]['bounds'][3], coords[1])
                                elif geom.get('type') in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']:
                                    # Simplified bounds calculation
                                    coords_list = geom['coordinates']
                                    def flatten_coords(coords):
                                        if isinstance(coords[0], (int, float)):
                                            return [coords]
                                        result = []
                                        for item in coords:
                                            result.extend(flatten_coords(item))
                                        return result
                                    
                                    all_coords = flatten_coords(coords_list)
                                    for coord in all_coords:
                                        if len(coord) >= 2:
                                            airports_data[airport_code]['bounds'][0] = min(airports_data[airport_code]['bounds'][0], coord[0])
                                            airports_data[airport_code]['bounds'][1] = min(airports_data[airport_code]['bounds'][1], coord[1])
                                            airports_data[airport_code]['bounds'][2] = max(airports_data[airport_code]['bounds'][2], coord[0])
                                            airports_data[airport_code]['bounds'][3] = max(airports_data[airport_code]['bounds'][3], coord[1])
                        
                        # Write back
                        with open(geojson_file, 'w') as f:
                            json.dump(data, f)
                except Exception as e:
                    print(f"Warning: Could not preprocess {geojson_file}: {e}")
            
            # Calculate center coordinates for each airport
            airports_list = []
            for code, data in airports_data.items():
                bounds = data['bounds']
                center_lon = (bounds[0] + bounds[2]) / 2
                center_lat = (bounds[1] + bounds[3]) / 2
                airports_list.append({
                    "code": code,
                    "center": [center_lon, center_lat],
                    "bounds": bounds,
                    "feature_count": data['count']
                })
            
            # Sort by airport code
            airports_list.sort(key=lambda x: x['code'])
            
            # Save airports to JSON file for API access
            # Use tileset_id as filename (sanitized)
            if output_mode == 'mapbox':
                airports_filename = f"{tileset_id.replace('.', '_').replace('/', '_')}_airports.json"
                airports_filepath = DATA_DIR / airports_filename
                with open(airports_filepath, 'w') as f:
                    json.dump(airports_list, f, indent=2)
                print(f"Saved {len(airports_list)} airports to {airports_filepath}")
            
            # Step 3: Group GeoJSON files by type (suffix after dash or underscore)
            files_by_type = defaultdict(list)
            
            for geojson_file in geojson_files:
                filename = geojson_file.stem
                
                # Extract the type (part after the last dash or underscore)
                if '-' in filename or '_' in filename:
                    normalized = filename.replace('_', '-')
                    layer_type = normalized.split('-')[-1]
                else:
                    layer_type = filename
                
                files_by_type[layer_type].append(geojson_file)
            
            # Track which layers are in this upload
            new_layers = list(files_by_type.keys())
            
            # Step 3: Process each type/layer
            layer_mbtiles_files = []
            
            for layer_type, files in files_by_type.items():
                if len(files) == 1:
                    # Single file for this type - create MBTiles directly
                    geojson_file = files[0]
                    layer_mbtiles_path = os.path.join(temp_mbtiles_dir, f"{layer_type}.mbtiles")
                    
                    command = [
                        'tippecanoe',
                        '-o', layer_mbtiles_path,
                        '-l', layer_type,
                        '-Z10',
                        '-z16',
                        '--force',
                        '--no-feature-limit',
                        '--no-tile-size-limit',
                        '--preserve-input-order',
                        '--drop-densest-as-needed',
                        str(geojson_file)
                    ]
                    
                    subprocess.run(
                        command,
                        check=True,
                        capture_output=True,
                        text=True
                    )
                    
                    layer_mbtiles_files.append(layer_mbtiles_path)
                else:
                    # Multiple files for this type - merge them into one layer
                    individual_mbtiles = []
                    
                    for idx, geojson_file in enumerate(files):
                        temp_individual_path = os.path.join(
                            temp_mbtiles_dir, 
                            f"{layer_type}_temp_{idx}.mbtiles"
                        )
                        
                        command = [
                            'tippecanoe',
                            '-o', temp_individual_path,
                            '-l', layer_type,
                            '-Z10',
                            '-z16',
                            '--force',
                            '--no-feature-limit',
                            '--no-tile-size-limit',
                            '--preserve-input-order',
                            '--drop-densest-as-needed',
                            str(geojson_file)
                        ]
                        
                        subprocess.run(
                            command,
                            check=True,
                            capture_output=True,
                            text=True
                        )
                        
                        individual_mbtiles.append(temp_individual_path)
                    
                    # Merge all individual MBTiles for this type into one
                    layer_mbtiles_path = os.path.join(temp_mbtiles_dir, f"{layer_type}.mbtiles")
                    
                    merge_command = [
                        'tile-join',
                        '-o', layer_mbtiles_path,
                        '--force'
                    ] + individual_mbtiles
                    
                    subprocess.run(
                        merge_command,
                        check=True,
                        capture_output=True,
                        text=True
                    )
                    
                    layer_mbtiles_files.append(layer_mbtiles_path)

            # Step 4: Merge all layer MBTiles into final output
            if len(layer_mbtiles_files) == 1:
                shutil.copy(layer_mbtiles_files[0], output_mbtiles_path)
            else:
                final_join_command = [
                    'tile-join',
                    '-o', output_mbtiles_path,
                    '--force'
                ] + layer_mbtiles_files
                
                subprocess.run(
                    final_join_command,
                    check=True,
                    capture_output=True,
                    text=True
                )
            
            # Step 4.5: Add airport metadata to MBTiles
            try:
                conn = sqlite3.connect(output_mbtiles_path)
                cursor = conn.cursor()
                
                # Store airports as custom metadata
                cursor.execute(
                    "INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)",
                    ('airports', json.dumps(airports_list))
                )
                
                conn.commit()
                conn.close()
                print(f"Added {len(airports_list)} airports to MBTiles metadata")
            except Exception as e:
                print(f"Warning: Could not add airport metadata: {e}")

            # Step 5: Handle output based on mode
            if output_mode == 'download':
                return send_file(
                    output_mbtiles_path,
                    as_attachment=True,
                    download_name='converted.mbtiles',
                    mimetype='application/vnd.mapbox-vector-tile'
                )
            
            elif output_mode == 'mapbox':
                try:
                    uploader = Uploader(access_token=mapbox_token)
                    
                    if update_mode == 'replace':
                        # REPLACE mode: Upload directly, overwriting the tileset
                        print(f"📤 Uploading to Mapbox: {tileset_id}")
                        with open(output_mbtiles_path, 'rb') as src:
                            upload_resp = uploader.upload(src, tileset_id)
                        
                        print(f"📥 Mapbox response: {upload_resp.status_code}")
                        
                        if upload_resp.status_code in [200, 201]:
                            print(f"✅ Mapbox upload successful, now saving MBTiles locally...")
                            # Save MBTiles for future production pushes
                            try:
                                # Ensure storage directory exists
                                MBTILES_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
                                storage_path = MBTILES_STORAGE_DIR / f"{tileset_id}.mbtiles"
                                
                                # Delete old file if exists (keep only latest)
                                if storage_path.exists():
                                    storage_path.unlink()
                                    print(f"🗑️  Deleted old MBTiles: {storage_path}")
                                
                                # Copy new MBTiles
                                shutil.copy2(output_mbtiles_path, storage_path)
                                file_size = storage_path.stat().st_size / (1024 * 1024)  # MB
                                print(f"✅ Saved MBTiles to {storage_path} ({file_size:.2f} MB)")
                            except Exception as e:
                                print(f"⚠️  Failed to save MBTiles: {e}")
                                import traceback
                                traceback.print_exc()
                            
                            return jsonify({
                                "success": True,
                                "message": "Tileset replaced successfully!",
                                "tileset_id": tileset_id,
                                "mapbox_url": f"https://studio.mapbox.com/tilesets/{tileset_id}/",
                                "mode": "replace",
                                "layers": new_layers,
                                "airports": airports_list
                            })
                        else:
                            return jsonify({
                                "error": "Mapbox API returned an error",
                                "status_code": upload_resp.status_code,
                                "details": upload_resp.text
                            }), 500
                    
                    elif update_mode == 'append':
                        # FEATURE-LEVEL SMART APPEND: Remove only matching airport features, keep everything else
                        existing_mbtiles_path = os.path.join(work_dir, 'existing.mbtiles')
                        merged_mbtiles_path = os.path.join(work_dir, 'merged.mbtiles')
                        
                        download_url = f"https://api.mapbox.com/tilesets/v1/{tileset_id}.mbtiles?access_token={mapbox_token}"
                        
                        try:
                            download_resp = requests.get(download_url, stream=True)
                            
                            if download_resp.status_code == 200:
                                # Save existing tileset
                                with open(existing_mbtiles_path, 'wb') as f:
                                    for chunk in download_resp.iter_content(chunk_size=8192):
                                        f.write(chunk)
                                
                                # Get airport IDs from new upload (already extracted earlier)
                                new_airport_ids = set(airports_list.keys()) if isinstance(airports_list, dict) else set()
                                
                                if not new_airport_ids:
                                    # Fallback: extract from GeoJSON files
                                    new_airport_ids = set()
                                    for geojson_file in geojson_files:
                                        with open(geojson_file, 'r') as f:
                                            data = json.load(f)
                                            for feature in data.get('features', []):
                                                airport_id = feature.get('properties', {}).get('airport_id')
                                                if airport_id:
                                                    new_airport_ids.add(airport_id)
                                
                                print(f"🏯 Airports in new upload: {new_airport_ids}")
                                
                                # Perform feature-level de-duplication
                                success = feature_level_deduplicate(
                                    existing_mbtiles_path=existing_mbtiles_path,
                                    new_geojson_files=geojson_files,
                                    new_airports=new_airport_ids,
                                    work_dir=work_dir,
                                    output_path=merged_mbtiles_path
                                )
                                
                                if not success:
                                    return jsonify({
                                        "error": "Feature-level de-duplication failed",
                                        "details": "Check server logs for more information"
                                    }), 500
                                
                                # Upload merged tileset
                                with open(merged_mbtiles_path, 'rb') as src:
                                    upload_resp = uploader.upload(src, tileset_id)

                                if upload_resp.status_code in [200, 201]:
                                    # Save merged MBTiles for future production pushes
                                    try:
                                        # Ensure storage directory exists
                                        MBTILES_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
                                        storage_path = MBTILES_STORAGE_DIR / f"{tileset_id}.mbtiles"
                                        
                                        # Delete old file if exists (keep only latest)
                                        if storage_path.exists():
                                            storage_path.unlink()
                                            print(f"🗑️  Deleted old MBTiles: {storage_path}")
                                        
                                        # Copy new MBTiles
                                        shutil.copy2(merged_mbtiles_path, storage_path)
                                        file_size = storage_path.stat().st_size / (1024 * 1024)  # MB
                                        print(f"✅ Saved merged MBTiles to {storage_path} ({file_size:.2f} MB)")
                                    except Exception as e:
                                        print(f"⚠️  Failed to save MBTiles: {e}")
                                        import traceback
                                        traceback.print_exc()
                                    
                                    message = "Feature-level smart append complete!"
                                    if new_airport_ids:
                                        message += f" Updated airports: {', '.join(sorted(new_airport_ids))}"
                                    
                                    return jsonify({
                                        "success": True,
                                        "message": message,
                                        "tileset_id": tileset_id,
                                        "mapbox_url": f"https://studio.mapbox.com/tilesets/{tileset_id}/",
                                        "mode": "append (feature-level smart)",
                                        "airports_updated": sorted(list(new_airport_ids)),
                                        "layers": new_layers,
                                        "airports": airports_list
                                    })
                                else:
                                    return jsonify({
                                        "error": "Failed to upload merged tileset",
                                        "status_code": upload_resp.status_code,
                                        "details": upload_resp.text
                                    }), 500
                            
                            elif download_resp.status_code == 404:
                                # Tileset doesn't exist yet, upload as new
                                with open(output_mbtiles_path, 'rb') as src:
                                    upload_resp = uploader.upload(src, tileset_id)

                                if upload_resp.status_code in [200, 201]:
                                    return jsonify({
                                        "success": True,
                                        "message": "Tileset created successfully (first upload)!",
                                        "tileset_id": tileset_id,
                                        "mapbox_url": f"https://studio.mapbox.com/tilesets/{tileset_id}/",
                                        "mode": "append (new tileset)",
                                        "layers": new_layers,
                                        "airports": airports_list
                                    })
                                else:
                                    return jsonify({
                                        "error": "Failed to create new tileset",
                                        "status_code": upload_resp.status_code,
                                        "details": upload_resp.text
                                    }), 500
                            
                            else:
                                return jsonify({
                                    "error": "Failed to download existing tileset",
                                    "status_code": download_resp.status_code,
                                    "details": download_resp.text
                                }), 500
                        
                        except requests.RequestException as e:
                            return jsonify({
                                "error": "Network error while downloading existing tileset",
                                "details": str(e)
                            }), 500

                except Exception as e:
                    return jsonify({
                        "error": "Failed to process Mapbox upload",
                        "details": str(e)
                    }), 500

        except subprocess.CalledProcessError as e:
            return jsonify({
                "error": "Error during tile processing",
                "command": ' '.join(e.cmd),
                "stderr": e.stderr,
                "stdout": e.stdout
            }), 500
        
        except Exception as e:
            return jsonify({
                "error": "An unexpected error occurred",
                "details": str(e)
            }), 500

@app.route('/api/push-to-production', methods=['POST'])
@require_auth
def push_to_production():
    """
    Push a staging tileset to production by re-uploading the stored MBTiles file.
    
    Request JSON:
    {
        "staging_tileset_id": "username.staging-abc123",
        "production_tileset_id": "username.production-tileset",
        "mode": "replace" or "append"
    }
    """
    try:
        data = request.json
        staging_tileset_id = data.get('staging_tileset_id')
        production_tileset_id = data.get('production_tileset_id')
        mode = data.get('mode', 'replace')
        mapbox_token = data.get('mapbox_token') or MAPBOX_SECRET_TOKEN
        
        if not staging_tileset_id or not production_tileset_id:
            return jsonify({
                'success': False,
                'error': 'Missing required fields: staging_tileset_id, production_tileset_id'
            }), 400
        
        if mode not in ['replace', 'append']:
            return jsonify({
                'success': False,
                'error': 'Invalid mode. Must be "replace" or "append"'
            }), 400
        
        # Find the MBTiles file for the staging tileset
        mbtiles_path = MBTILES_STORAGE_DIR / f"{staging_tileset_id}.mbtiles"
        
        if not mbtiles_path.exists():
            return jsonify({
                'success': False,
                'error': f'MBTiles file not found for {staging_tileset_id}. Please re-upload your data to staging first.',
                'mbtiles_path': str(mbtiles_path)
            }), 404
        
        # Upload to production
        uploader = Uploader(access_token=mapbox_token)
        
        if mode == 'replace':
            # REPLACE mode: Upload directly, overwriting the production tileset
            with open(mbtiles_path, 'rb') as src:
                upload_resp = uploader.upload(src, production_tileset_id)
            
            if upload_resp.status_code in [200, 201]:
                # Save MBTiles for production tileset too
                try:
                    # Ensure storage directory exists
                    MBTILES_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
                    prod_storage_path = MBTILES_STORAGE_DIR / f"{production_tileset_id}.mbtiles"
                    
                    # Delete old file if exists (keep only latest)
                    if prod_storage_path.exists():
                        prod_storage_path.unlink()
                        print(f"🗑️  Deleted old production MBTiles: {prod_storage_path}")
                    
                    # Copy MBTiles
                    shutil.copy2(mbtiles_path, prod_storage_path)
                    file_size = prod_storage_path.stat().st_size / (1024 * 1024)  # MB
                    print(f"✅ Saved production MBTiles to {prod_storage_path} ({file_size:.2f} MB)")
                except Exception as e:
                    print(f"⚠️  Failed to save production MBTiles: {e}")
                    import traceback
                    traceback.print_exc()
                
                return jsonify({
                    'success': True,
                    'message': f'Successfully pushed to {production_tileset_id} (replace mode)',
                    'tileset_id': production_tileset_id,
                    'mapbox_url': f'https://studio.mapbox.com/tilesets/{production_tileset_id}/',
                    'mode': 'replace'
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Mapbox API returned an error',
                    'status_code': upload_resp.status_code,
                    'details': upload_resp.text
                }), 500
        
        elif mode == 'append':
            # APPEND mode: Download existing production, merge, then upload
            with tempfile.TemporaryDirectory() as work_dir:
                existing_path = os.path.join(work_dir, 'existing.mbtiles')
                merged_path = os.path.join(work_dir, 'merged.mbtiles')
                
                download_url = f"https://api.mapbox.com/tilesets/v1/{production_tileset_id}.mbtiles?access_token={mapbox_token}"
                
                try:
                    download_resp = requests.get(download_url, stream=True)
                    
                    if download_resp.status_code == 200:
                        # Save existing production tileset
                        with open(existing_path, 'wb') as f:
                            for chunk in download_resp.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        # FEATURE-LEVEL SMART APPEND: Extract airport IDs from staging MBTiles
                        # Decode staging MBTiles to get airport IDs
                        staging_airport_ids = set()
                        staging_layers = get_mbtiles_layers(str(mbtiles_path))
                        
                        for layer in staging_layers:
                            decode_cmd = [
                                'tippecanoe-decode',
                                '-l', layer,
                                str(mbtiles_path)
                            ]
                            
                            try:
                                result = subprocess.run(
                                    decode_cmd,
                                    capture_output=True,
                                    text=True
                                )
                                
                                if result.returncode == 0:
                                    data = json.loads(result.stdout)
                                    for feature in data.get('features', []):
                                        airport_id = feature.get('properties', {}).get('airport_id')
                                        if airport_id:
                                            staging_airport_ids.add(airport_id)
                            except Exception as e:
                                print(f"⚠️  Warning: Failed to decode staging layer {layer}: {e}")
                        
                        print(f"🏯 Airports in staging: {staging_airport_ids}")
                        
                        # Decode staging MBTiles to GeoJSON files
                        staging_geojson_dir = os.path.join(work_dir, 'staging_geojson')
                        os.makedirs(staging_geojson_dir, exist_ok=True)
                        staging_geojson_files = []
                        
                        for layer in staging_layers:
                            layer_geojson = os.path.join(staging_geojson_dir, f"{layer}.geojson")
                            decode_cmd = [
                                'tippecanoe-decode',
                                '-l', layer,
                                str(mbtiles_path)
                            ]
                            
                            with open(layer_geojson, 'w') as f:
                                result = subprocess.run(
                                    decode_cmd,
                                    stdout=f,
                                    stderr=subprocess.PIPE,
                                    text=True
                                )
                            
                            if result.returncode == 0:
                                staging_geojson_files.append(layer_geojson)
                        
                        # Perform feature-level de-duplication
                        success = feature_level_deduplicate(
                            existing_mbtiles_path=existing_path,
                            new_geojson_files=staging_geojson_files,
                            new_airports=staging_airport_ids,
                            work_dir=work_dir,
                            output_path=merged_path
                        )
                        
                        if not success:
                            return jsonify({
                                'success': False,
                                'error': 'Feature-level de-duplication failed for production push',
                                'details': 'Check server logs for more information'
                            }), 500
                        
                        # Upload merged tileset
                        with open(merged_path, 'rb') as src:
                            upload_resp = uploader.upload(src, production_tileset_id)
                        
                        if upload_resp.status_code in [200, 201]:
                            # Save merged MBTiles for production
                            try:
                                # Ensure storage directory exists
                                MBTILES_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
                                prod_storage_path = MBTILES_STORAGE_DIR / f"{production_tileset_id}.mbtiles"
                                
                                # Delete old file if exists (keep only latest)
                                if prod_storage_path.exists():
                                    prod_storage_path.unlink()
                                    print(f"🗑️  Deleted old production MBTiles: {prod_storage_path}")
                                
                                # Copy merged MBTiles
                                shutil.copy2(merged_path, prod_storage_path)
                                file_size = prod_storage_path.stat().st_size / (1024 * 1024)  # MB
                                print(f"✅ Saved merged production MBTiles to {prod_storage_path} ({file_size:.2f} MB)")
                            except Exception as e:
                                print(f"⚠️  Failed to save production MBTiles: {e}")
                                import traceback
                                traceback.print_exc()
                            
                            message = f'Successfully pushed to {production_tileset_id} (feature-level append)'
                            if staging_airport_ids:
                                message += f" - Updated airports: {', '.join(sorted(staging_airport_ids))}"
                            
                            return jsonify({
                                'success': True,
                                'message': message,
                                'tileset_id': production_tileset_id,
                                'mapbox_url': f'https://studio.mapbox.com/tilesets/{production_tileset_id}/',
                                'mode': 'append (feature-level smart)',
                                'airports_updated': sorted(list(staging_airport_ids))
                            })
                        else:
                            return jsonify({
                                'success': False,
                                'error': 'Failed to upload merged tileset',
                                'status_code': upload_resp.status_code,
                                'details': upload_resp.text
                            }), 500
                    
                    elif download_resp.status_code == 404:
                        # Production tileset doesn't exist, upload as new
                        with open(mbtiles_path, 'rb') as src:
                            upload_resp = uploader.upload(src, production_tileset_id)
                        
                        if upload_resp.status_code in [200, 201]:
                            # Save MBTiles for production
                            try:
                                # Ensure storage directory exists
                                MBTILES_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
                                prod_storage_path = MBTILES_STORAGE_DIR / f"{production_tileset_id}.mbtiles"
                                
                                # Delete old file if exists (keep only latest)
                                if prod_storage_path.exists():
                                    prod_storage_path.unlink()
                                    print(f"🗑️  Deleted old production MBTiles: {prod_storage_path}")
                                
                                # Copy MBTiles
                                shutil.copy2(mbtiles_path, prod_storage_path)
                                file_size = prod_storage_path.stat().st_size / (1024 * 1024)  # MB
                                print(f"✅ Saved production MBTiles to {prod_storage_path} ({file_size:.2f} MB)")
                            except Exception as e:
                                print(f"⚠️  Failed to save production MBTiles: {e}")
                                import traceback
                                traceback.print_exc()
                            
                            return jsonify({
                                'success': True,
                                'message': f'Successfully created {production_tileset_id} (production tileset did not exist)',
                                'tileset_id': production_tileset_id,
                                'mapbox_url': f'https://studio.mapbox.com/tilesets/{production_tileset_id}/',
                                'mode': 'append (new)'
                            })
                        else:
                            return jsonify({
                                'success': False,
                                'error': 'Failed to create production tileset',
                                'status_code': upload_resp.status_code,
                                'details': upload_resp.text
                            }), 500
                    
                    else:
                        return jsonify({
                            'success': False,
                            'error': 'Failed to download existing production tileset',
                            'status_code': download_resp.status_code
                        }), 500
                
                except subprocess.CalledProcessError as e:
                    return jsonify({
                        'success': False,
                        'error': 'Failed to merge tilesets',
                        'details': e.stderr
                    }), 500
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'An unexpected error occurred',
            'details': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

