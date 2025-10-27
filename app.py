import os
import subprocess
import tempfile
import zipfile
import requests
from pathlib import Path
from flask import Flask, request, render_template, send_file, jsonify
from mapbox import Uploader

app = Flask(__name__)

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size
ALLOWED_EXTENSIONS = {'zip'}

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Render the upload form."""
    return render_template('index.html')

@app.route('/health')
def health():
    """Health check endpoint for Railway."""
    return jsonify({"status": "healthy"}), 200

@app.route('/upload', methods=['POST'])
def upload_and_process():
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
    
    # Validate Mapbox parameters if needed
    if output_mode == 'mapbox':
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

            # Step 2: Group GeoJSON files by type (suffix after dash or underscore)
            # Example: e16-rwy.geojson, IGN-rwy.geojson, goo_rwy.geojson all go into 'rwy' layer
            from collections import defaultdict
            files_by_type = defaultdict(list)
            
            for geojson_file in geojson_files:
                filename = geojson_file.stem  # e.g., "e16-rwy", "IGN-rwy", or "goo_aim"
                
                # Extract the type (part after the last dash or underscore)
                # Normalize by replacing underscores with dashes
                if '-' in filename or '_' in filename:
                    normalized = filename.replace('_', '-')
                    layer_type = normalized.split('-')[-1]  # e.g., "rwy", "aim"
                else:
                    # If no separator, use the whole filename as the type
                    layer_type = filename
                
                files_by_type[layer_type].append(geojson_file)
            
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
                        '--force',
                        '--drop-densest-as-needed',
                        '--extend-zooms-if-still-dropping',
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
                    # First, create individual MBTiles for each file
                    individual_mbtiles = []
                    
                    for idx, geojson_file in enumerate(files):
                        temp_individual_path = os.path.join(
                            temp_mbtiles_dir, 
                            f"{layer_type}_temp_{idx}.mbtiles"
                        )
                        
                        command = [
                            'tippecanoe',
                            '-o', temp_individual_path,
                            '-l', layer_type,  # Same layer name for all
                            '--force',
                            '--drop-densest-as-needed',
                            '--extend-zooms-if-still-dropping',
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
                # If there's only one layer, just copy it
                import shutil
                shutil.copy(layer_mbtiles_files[0], output_mbtiles_path)
            else:
                # Merge multiple layers into final MBTiles
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

            # Step 5: Handle output based on mode
            if output_mode == 'download':
                # Send the MBTiles file to the user for download
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
                        with open(output_mbtiles_path, 'rb') as src:
                            upload_resp = uploader.upload(src, tileset_id)

                        if upload_resp.status_code in [200, 201]:
                            return jsonify({
                                "success": True,
                                "message": "Tileset replaced successfully!",
                                "tileset_id": tileset_id,
                                "mapbox_url": f"https://studio.mapbox.com/tilesets/{tileset_id}/",
                                "mode": "replace"
                            })
                        else:
                            return jsonify({
                                "error": "Mapbox API returned an error",
                                "status_code": upload_resp.status_code,
                                "details": upload_resp.text
                            }), 500
                    
                    elif update_mode == 'append':
                        # APPEND mode: Download existing, merge, then upload
                        existing_mbtiles_path = os.path.join(work_dir, 'existing.mbtiles')
                        merged_mbtiles_path = os.path.join(work_dir, 'merged.mbtiles')
                        
                        # Download existing tileset from Mapbox
                        download_url = f"https://api.mapbox.com/tilesets/v1/{tileset_id}.mbtiles?access_token={mapbox_token}"
                        
                        try:
                            download_resp = requests.get(download_url, stream=True)
                            
                            if download_resp.status_code == 200:
                                # Save existing tileset
                                with open(existing_mbtiles_path, 'wb') as f:
                                    for chunk in download_resp.iter_content(chunk_size=8192):
                                        f.write(chunk)
                                
                                # Merge existing with new using tile-join
                                merge_append_command = [
                                    'tile-join',
                                    '-o', merged_mbtiles_path,
                                    '--force',
                                    existing_mbtiles_path,
                                    output_mbtiles_path
                                ]
                                
                                subprocess.run(
                                    merge_append_command,
                                    check=True,
                                    capture_output=True,
                                    text=True
                                )
                                
                                # Upload merged tileset back to Mapbox
                                with open(merged_mbtiles_path, 'rb') as src:
                                    upload_resp = uploader.upload(src, tileset_id)

                                if upload_resp.status_code in [200, 201]:
                                    return jsonify({
                                        "success": True,
                                        "message": "New data appended to tileset successfully!",
                                        "tileset_id": tileset_id,
                                        "mapbox_url": f"https://studio.mapbox.com/tilesets/{tileset_id}/",
                                        "mode": "append"
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
                                        "mode": "append (new tileset)"
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

if __name__ == '__main__':
    # For local development
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

