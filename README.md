# GeoJSON to MBTiles Converter

A web application that converts ZIP archives of GeoJSON files into a single MBTiles file, with optional direct upload to Mapbox.

## Features

- **Batch Conversion**: Upload a ZIP file containing multiple GeoJSON files
- **Automatic Layer Creation**: Each GeoJSON file becomes a separate layer in the final MBTiles
- **Mapbox Integration**: Optionally upload the result directly to your Mapbox account
- **Production Ready**: Built with Docker and designed for deployment on Railway

## How It Works

1. User uploads a ZIP file containing `.geojson` files
2. The application extracts and processes each GeoJSON file using `tippecanoe`
3. Individual MBTiles files are merged into a single file using `tile-join`
4. The result is either:
   - Downloaded directly to the user's computer, or
   - Uploaded automatically to Mapbox (if credentials are provided)

## Technology Stack

- **Backend**: Python 3.10 + Flask
- **Tile Processing**: Tippecanoe (built from source)
- **Container**: Multi-stage Docker build
- **Deployment**: Railway (or any Docker-compatible platform)

## Local Development

### Prerequisites

- Docker installed on your machine

### Running Locally

1. Clone this repository
2. Build the Docker image:
   ```bash
   docker build -t geojson-mbtiles .
   ```
3. Run the container:
   ```bash
   docker run -p 8080:8080 geojson-mbtiles
   ```
4. Open your browser to `http://localhost:8080`

## Deployment on Railway

1. Push this code to a GitHub repository
2. Create a new project on [Railway](https://railway.app)
3. Select "Deploy from GitHub repo"
4. Choose your repository
5. Railway will automatically detect the `Dockerfile` and deploy

### Environment Variables (Optional)

No environment variables are required for basic operation. Mapbox credentials are provided by users through the web interface.

## API Endpoints

### `GET /`
Renders the upload form interface.

### `POST /upload`
Processes the uploaded ZIP file.

**Form Parameters:**
- `file` (required): ZIP file containing GeoJSON files
- `mapbox_token` (optional): Mapbox secret access token
- `mapbox_username` (optional): Mapbox username
- `tileset_name` (optional): Name for the new tileset

**Response:**
- If Mapbox credentials provided: JSON with upload status and tileset URL
- If no Mapbox credentials: Downloads the MBTiles file

### `GET /health`
Health check endpoint for monitoring.

## Security Notes

- Maximum upload size: 500MB
- Mapbox tokens are never stored on the server
- All processing happens in temporary directories that are automatically cleaned up
- The application runs in an isolated Docker container

## License

MIT

## Credits

Built with:
- [Tippecanoe](https://github.com/felt/tippecanoe) by Felt
- [Flask](https://flask.palletsprojects.com/)
- [Mapbox Python SDK](https://github.com/mapbox/mapbox-sdk-py)

