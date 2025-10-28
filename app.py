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

@app.route('/')
def index():
    """Render the main upload page."""
    return render_template('index.html',
                         default_staging_tileset=DEFAULT_STAGING_TILESET,
                         mapbox_secret_token=MAPBOX_SECRET_TOKEN,
                         mapbox_public_token=MAPBOX_PUBLIC_TOKEN)

@app.route('/viewer')
def viewer():
    """Render the viewer page for managing tilesets."""
    return render_template('viewer.html',
                         default_staging_tileset=DEFAULT_STAGING_TILESET,
                         default_production_tileset=DEFAULT_PRODUCTION_TILESET,
                         mapbox_secret_token=MAPBOX_SECRET_TOKEN,
                         mapbox_public_token=MAPBOX_PUBLIC_TOKEN)

@app.route('/api/authenticate', methods=['POST'])
def authenticate():
    """Authenticate user with passcode."""
    data = request.json
    passcode = data.get('passcode', '')
    
    if passcode == APP_PASSCODE:
        session['authenticated'] = True
        return jsonify({'success': True, 'authenticated': True})
    else:
        return jsonify({'success': False, 'error': 'Invalid passcode'}), 401

@app.route('/api/check-auth', methods=['GET'])
def check_auth():
    """Check if user is authenticated."""
    return jsonify({'authenticated': session.get('authenticated', False)})

@app.route('/api/logout', methods=['POST'])
def logout():
    """Log out the user."""
    session.pop('authenticated', None)
    return jsonify({'success': True, 'authenticated': False})

@app.route('/upload', methods=['POST'])
@require_auth
def upload():
    """
    Handle file upload and conversion to MBTiles.
    Supports both download and Mapbox upload modes.
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type. Please upload a .zip file"}), 400

    # Get output mode and related parameters
    output_mode = request.form.get('output_mode', 'download').strip()
    
    # Mapbox-specific parameters
    tileset_id = request.form.get('tileset_id', '').strip()
    update_mode = request.form.get('update_mode', 'replace').strip()
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
                                        """Recursively flatten coordinate arrays."""
                                        result = []
                                        for item in coords:
                                            if isinstance(item, list) and len(item) > 0 and isinstance(item[0], list):
                                                result.extend(flatten_coords(item))
                                            else:
                                                result.append(item)
                                        return result
                                    
                                    flat_coords = flatten_coords(coords_list)
                                    for coord in flat_coords:
                                        if len(coord) >= 2:
                                            airports_data[airport_code]['bounds'][0] = min(airports_data[airport_code]['bounds'][0], coord[0])
                                            airports_data[airport_code]['bounds'][1] = min(airports_data[airport_code]['bounds'][1], coord[1])
                                            airports_data[airport_code]['bounds'][2] = max(airports_data[airport_code]['bounds'][2], coord[0])
                                            airports_data[airport_code]['bounds'][3] = max(airports_data[airport_code]['bounds'][3], coord[1])
                        
                        # Write back the modified GeoJSON
                        with open(geojson_file, 'w') as f:
                            json.dump(data, f)
                
                except Exception as e:
                    print(f"Warning: Could not process {geojson_file}: {e}")
                    continue

            # Convert airports_data to list format for response
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

            # Step 3: Group files by layer type and convert to MBTiles
            files_by_type = defaultdict(list)
            
            for geojson_file in geojson_files:
                filename = geojson_file.stem
                
                # Determine layer type from filename
                if '-' in filename:
                    layer_type = filename.split('-', 1)[1]
                elif '_' in filename:
                    layer_type = filename.split('_', 1)[1]
                else:
                    layer_type = filename
                
                files_by_type[layer_type].append(geojson_file)
            
            # Track which layers are in this upload
            new_layers = list(files_by_type.keys())
            
            layer_mbtiles_files = []
            
            for layer_type, files in files_by_type.items():
                print(f"🔨 Processing layer '{layer_type}' with {len(files)} file(s)")
                
                if len(files) == 1:
                    # Single file - convert directly
                    geojson_file = files[0]
                    layer_mbtiles_path = os.path.join(temp_mbtiles_dir, f"{layer_type}.mbtiles")
                    
                    subprocess.run(
                        [
                            'tippecanoe',
                            '-o', layer_mbtiles_path,
                            '--force',
                            '--no-tile-compression',
                            '--maximum-zoom=18',
                            '--minimum-zoom=0',
                            '--drop-densest-as-needed',
                            '--extend-zooms-if-still-dropping',
                            '-l', layer_type,
                            str(geojson_file)
                        ],
                        check=True,
                        capture_output=True,
                        text=True
                    )
                    
                    layer_mbtiles_files.append(layer_mbtiles_path)
                else:
                    # Multiple files for this type - create individual MBTiles then merge
                    individual_mbtiles = []
                    
                    for idx, geojson_file in enumerate(files):
                        temp_individual_path = os.path.join(temp_mbtiles_dir, f"{layer_type}_{idx}.mbtiles")
                        
                        subprocess.run(
                            [
                                'tippecanoe',
                                '-o', temp_individual_path,
                                '--force',
                                '--no-tile-compression',
                                '--maximum-zoom=18',
                                '--minimum-zoom=0',
                                '--drop-densest-as-needed',
                                '--extend-zooms-if-still-dropping',
                                '-l', layer_type,
                                str(geojson_file)
                            ],
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
                final_merge_command = [
                    'tile-join',
                    '-o', output_mbtiles_path,
                    '--force'
                ] + layer_mbtiles_files
                
                subprocess.run(
                    final_merge_command,
                    check=True,
                    capture_output=True,
                    text=True
                )

            print(f"✅ Successfully created MBTiles with {len(layer_mbtiles_files)} layers")

            # Handle output based on mode
            if output_mode == 'download':
                # Return the MBTiles file for download
                return send_file(
                    output_mbtiles_path,
                    as_attachment=True,
                    download_name='converted.mbtiles',
                    mimetype='application/x-mbtiles'
                )
            
            elif output_mode == 'mapbox':
                # Upload to Mapbox
                uploader = Uploader(access_token=mapbox_token)
                
                # REPLACE mode only - upload directly, overwriting the tileset
                with open(output_mbtiles_path, 'rb') as src:
                    upload_resp = uploader.upload(src, tileset_id)

                if upload_resp.status_code in [200, 201]:
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
                        "message": "Tileset uploaded successfully (replace mode)!",
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

        except subprocess.CalledProcessError as e:
            error_message = e.stderr if e.stderr else str(e)
            return jsonify({
                "error": "Tippecanoe conversion failed",
                "details": error_message
            }), 500
        except Exception as e:
            import traceback
            traceback.print_exc()
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
        "mode": "replace"
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
        
        if mode != 'replace':
            return jsonify({
                'success': False,
                'error': 'Invalid mode. Only "replace" is supported'
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
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'An unexpected error occurred',
            'details': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
