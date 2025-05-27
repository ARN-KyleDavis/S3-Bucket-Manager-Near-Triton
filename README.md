# S3 Bucket Near Triton Integration

This Flask application provides an interface for managing and transferring files between Near and Triton S3 buckets, with additional features for file processing and database integration.

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- AWS CLI configured with appropriate credentials
- PostgreSQL database (if using database features)

## Installation

1. Clone the repository:
```bash
git clone <your-repository-url>
cd S3-Bucket-Near-Triton
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
# On Windows
venv\Scripts\activate
# On Unix or MacOS
source venv/bin/activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root directory with the following variables:
```env
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
TRITON_ACCESS_KEY=your_triton_access_key
TRITON_SECRET_ACCESS_KEY=your_triton_secret_key
NEAR_SECRET_ACCESS_KEY=your_near_secret_key
NEAR_ACCESS_KEY_ID=your_near_access_key
```

## Usage

1. Start the Flask application:
```bash
python main.py
```

The server will start on `http://localhost:3000`

### Available Endpoints

- `GET /list-bucket`: List files in a specified S3 bucket
- `GET /get-object`: Download a specific object from S3
- `GET /download-to-server`: Download a file from S3 to the server
- `GET /download/<filename>`: Download a processed file
- `POST /add-file`: Upload a file to S3
- `POST /delete-object`: Delete a specific object from S3
- `POST /delete-all-files`: Delete all files in a bucket
- `GET /update-database`: Update database with latest TSV data
- `GET /upload-latest-to-s3`: Upload the latest file to S3
- `POST /local-upload-to-folder`: Upload a local file to an S3 folder
- `GET /manual-upload-to-triton`: Manually upload a file to Triton
- `GET /list-triton-files`: List files in the Triton bucket
- `GET /validate-taxonomy`: Validate taxonomy against segment data

## Directory Structure

- `assets/`: Directory for storing downloaded and processed files
- `temp/`: Temporary storage for file processing
- `uploads/`: Directory for files to be uploaded

## Azure DevOps Integration

1. Create a new repository in Azure DevOps
2. Initialize git in your local project:
```bash
git init
```

3. Add the Azure DevOps remote:
```bash
git remote add origin https://dev.azure.com/<your-organization>/<your-project>/_git/<repository-name>
```

4. Add your files and make your first commit:
```bash
git add .
git commit -m "Initial commit"
```

5. Push to Azure DevOps:
```bash
git push -u origin main
```

## Contributing

1. Create a new branch for your feature:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and commit them:
```bash
git add .
git commit -m "Description of your changes"
```

3. Push your branch to Azure DevOps:
```bash
git push origin feature/your-feature-name
```

4. Create a Pull Request in Azure DevOps

## Security Notes

- Never commit the `.env` file or any files containing sensitive credentials
- Keep your AWS and database credentials secure
- Regularly rotate your access keys and passwords

## Support

For support, please contact your system administrator or create an issue in the Azure DevOps repository. 