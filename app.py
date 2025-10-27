import os
import subprocess
import tempfile
import zipfile
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

    # Get Mapbox details from the form (optional)
    mapbox_token = request.form.get('mapbox_token', '').strip()
    mapbox_username = request.form.get('mapbox_username', '').strip()
    tileset_name = request.form.get('tileset_name', '').strip()
    
    # Determine if we should upload to Mapbox
    upload_to_mapbox = bool(mapbox_token and mapbox_username and tileset_name)
    
    # If Mapbox fields are partially filled, return an error
    mapbox_fields = [mapbox_token, mapbox_username, tileset_name]
    if any(mapbox_fields) and not all(mapbox_fields):
        return jsonify({
            "error": "If uploading to Mapbox, all three fields (token, username, tileset name) are required"
        }), 400

    # Create temporary directory for processing
    with tempfile.TemporaryDirectory() as work_dir:
        try:
            # Set up paths
            input_zip_path = os.path.join(work_dir, 'input.zip')
            geojson_dir = os.path.join(work_dir, 'geojson_files')
            temp_mbtiles_dir = os.path.join(work_dir, 'temp_mbtiles')
            output_mbtiles_path = os.path.join(work_dir, 'output.mbtiles')

            os.makedirs(geojson_dir)
            os.makedirs(temp_mbtiles_dir)
            
            # Save the uploaded file
            file.save(input_zip_path)

            # Step 1: Unzip the uploaded file
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

            # Step 2: Run tippecanoe for each GeoJSON file to create individual MBTiles
            temp_mbtiles_files = []
            for geojson_file in geojson_files:
                # Use the file stem (filename without extension) as the layer name
                layer_name = geojson_file.stem
                temp_mbtiles_path = os.path.join(temp_mbtiles_dir, f"{layer_name}.mbtiles")
                
                # Run tippecanoe
                command = [
                    'tippecanoe',
                    '-o', temp_mbtiles_path,
                    '-l', layer_name,
                    '--force',
                    '--drop-densest-as-needed',
                    '--extend-zooms-if-still-dropping',
                    str(geojson_file)
                ]
                
                result = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True
                )
                
                temp_mbtiles_files.append(temp_mbtiles_path)

            # Step 3: Run tile-join to merge all MBTiles files into one
            if len(temp_mbtiles_files) == 1:
                # If there's only one file, just copy it
                import shutil
                shutil.copy(temp_mbtiles_files[0], output_mbtiles_path)
            else:
                # Merge multiple files
                join_command = [
                    'tile-join',
                    '-o', output_mbtiles_path,
                    '--force'
                ] + temp_mbtiles_files
                
                subprocess.run(
                    join_command,
                    check=True,
                    capture_output=True,
                    text=True
                )

            # Step 4: Either upload to Mapbox or send file to user
            if upload_to_mapbox:
                # Sanitize tileset name for the ID (must be alphanumeric, underscores, hyphens)
                tileset_id = "".join(
                    c for c in tileset_name.lower().replace(" ", "-") 
                    if c.isalnum() or c in "-_"
                )
                final_tileset_id = f"{mapbox_username}.{tileset_id}"

                try:
                    # Upload to Mapbox using the SDK
                    uploader = Uploader(access_token=mapbox_token)
                    
                    with open(output_mbtiles_path, 'rb') as src:
                        upload_resp = uploader.upload(src, final_tileset_id)

                    if upload_resp.status_code in [200, 201]:
                        response_data = upload_resp.json()
                        return jsonify({
                            "success": True,
                            "message": "Upload successful! Mapbox is now processing your tileset.",
                            "tileset_id": final_tileset_id,
                            "mapbox_url": f"https://studio.mapbox.com/tilesets/{final_tileset_id}/",
                            "details": response_data
                        })
                    else:
                        return jsonify({
                            "error": "Mapbox API returned an error",
                            "status_code": upload_resp.status_code,
                            "details": upload_resp.text
                        }), 500

                except Exception as e:
                    return jsonify({
                        "error": "Failed to upload to Mapbox",
                        "details": str(e)
                    }), 500
            else:
                # Send the MBTiles file to the user for download
                return send_file(
                    output_mbtiles_path,
                    as_attachment=True,
                    download_name='converted.mbtiles',
                    mimetype='application/vnd.mapbox-vector-tile'
                )

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

@app.route('/health')
def health():
    """Health check endpoint for Railway."""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

