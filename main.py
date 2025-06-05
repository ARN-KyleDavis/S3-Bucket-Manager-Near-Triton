from flask import Flask, request, jsonify, Response, send_from_directory
import boto3
from dotenv import load_dotenv
import os
import gzip
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime
import psycopg2
import re


app = Flask(__name__)

load_dotenv()

def list_files(bucket_name):
  # Retrieve AWS credentials from environment variables
  access_key = os.getenv('AWS_ACCESS_KEY_ID')
  secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

  # Create an S3 client
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-southeast-2')  # Sydney region

  # List files in the specified S3 bucket
  try:
      response = s3.list_objects(Bucket=bucket_name)
      if 'Contents' not in response:
          return []

      files_info = []
      for file in response['Contents']:
          file_info = {
              'Key': file['Key'],
              'LastModified': file['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
              'Size': file['Size'],
              # You can add more metadata here if needed
          }
          files_info.append(file_info)

      return files_info
  except Exception as e:
      return f"Error accessing bucket {bucket_name}: {e}"

@app.route('/list-bucket', methods=['GET'])
def list_bucket():
        bucket_name = request.args.get('bucket-name')
        if not bucket_name:
            return jsonify({'error': 'No bucket name provided'}), 400
        files = list_files(bucket_name)
        return jsonify({'files': files})



@app.route('/get-object', methods=['GET'])
def get_object():
    bucket_name = request.args.get('bucket-name')
    object_key = request.args.get('object-key')

    if not bucket_name or not object_key:
        return jsonify({'error': 'Missing bucket name or object key'}), 400

    try:
        file_path = download_file_from_s3(bucket_name, object_key)
        return jsonify({'message': f'File {object_key} downloaded successfully to {file_path}'})
    except Exception as e:
        return jsonify({'error': f"Error retrieving object {object_key} from bucket {bucket_name}: {e}"}), 500

def download_file_from_s3(bucket_name, object_key):
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-southeast-2')

    object = s3.get_object(Bucket=bucket_name, Key=object_key)
    file_content = object['Body'].read()

    assets_dir = '/assets'  # Ensure this directory exists and your app has write permissions
    file_path = os.path.join(assets_dir, object_key)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'wb') as file:
        file.write(file_content)

    return file_path

@app.route('/download-to-server', methods=['GET'])
def download_to_server():
    bucket_name = request.args.get('bucket-name')
    object_key = request.args.get('object-key')

    # Check for required parameters
    if not bucket_name or not object_key:
        return jsonify({'error': 'Missing bucket name or object key'}), 400

    # Ensure the "assets" directory exists
    assets_dir = 'assets'
    if not os.path.exists(assets_dir):
        os.makedirs(assets_dir)

    # File path to save the downloaded file
    file_path = os.path.join(assets_dir, secure_filename(object_key))

    try:
        # Download the file from S3 and save it locally
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-southeast-2')
        s3.download_file(bucket_name, object_key, file_path)

        return jsonify({'message': f'File {object_key} downloaded successfully to server at {file_path}'})
    except Exception as e:
        return jsonify({'error': f"Error downloading object {object_key} from bucket {bucket_name}: {e}"}), 500

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    directory = os.getcwd() + '/assets'  # Path to your assets directory
    safe_filename = secure_filename(filename)

    # Full path to the gzip file
    gzip_file_path = os.path.join(directory, safe_filename)

    # Check if the .gz file exists
    if not os.path.exists(gzip_file_path):
        return jsonify({'error': 'File not found'}), 404

    # Assuming the filename is in the format 'file_name.gz'
    # Extract the base name (without .gz) to name the output CSV
    base_filename = safe_filename.rsplit('.', 1)[0]  # Remove .gz extension
    csv_filename = base_filename + '.csv'
    csv_file_path = os.path.join(directory, csv_filename)

    # Unzip the .gz file, read only the first 1000 lines of the TSV, and convert it to CSV
    with gzip.open(gzip_file_path, 'rt') as gzip_file:
        # Use pandas to read the TSV, with nrows set to 1000 to limit to the first 1000 lines
        df = pd.read_csv(gzip_file, sep='\t', nrows=1000)
        df.to_csv(csv_file_path, index=False)

    # Serve the limited CSV file for download
    return send_from_directory(directory, csv_filename, as_attachment=True)


@app.route('/add-file', methods=['POST'])
def add_file():
  if 'file' not in request.files:
      return jsonify({'error': 'No file part'}), 400
  file = request.files['file']
  if file.filename == '':
      return jsonify({'error': 'No selected file'}), 400

  bucket_name = request.form.get('bucket-name')
  if not bucket_name:
      return jsonify({'error': 'No bucket name provided'}), 400

  response = upload_file_to_s3(bucket_name, file)
  return jsonify({'message': response})

def upload_file_to_s3(bucket_name, file):
  # Retrieve AWS credentials from environment variables
  access_key = os.getenv('AWS_ACCESS_KEY_ID')
  secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

  # Create an S3 client
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-southeast-2')  # Sydney region

  # Upload file to the specified S3 bucket
  try:
      s3.upload_fileobj(file, bucket_name, file.filename)
      return f"File {file.filename} uploaded successfully to {bucket_name}"
  except Exception as e:
      return f"Error uploading file to bucket {bucket_name}: {e}"

@app.route('/delete-object', methods=['POST'])
def delete_object():
    data = request.json
    bucket_name = data.get('bucket-name')
    object_key = data.get('object-key')

    if not bucket_name:
        return jsonify({'error': 'No bucket name provided'}), 400
    if not object_key:
        return jsonify({'error': 'No object key provided'}), 400

    response = delete_object_from_s3(bucket_name, object_key)
    return jsonify({'message': response})

def delete_object_from_s3(bucket_name, object_key):
    # Retrieve AWS credentials from environment variables
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    # Create an S3 client
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-southeast-2')

    try:
        # Delete the specified object from the S3 bucket
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        return f"Object {object_key} deleted successfully from bucket {bucket_name}"
    except Exception as e:
        return f"Error deleting object {object_key} from bucket {bucket_name}: {e}"


@app.route('/delete-all-files', methods=['POST'])
def delete_all_files():
    data = request.json
    bucket_name = data.get('bucket-name')
    if not bucket_name:
        return jsonify({'error': 'No bucket name provided'}), 400

    response = delete_files_in_bucket(bucket_name)
    return jsonify({'message': response})

def delete_files_in_bucket(bucket_name):
  # Retrieve AWS credentials from environment variables
  access_key = os.getenv('AWS_ACCESS_KEY_ID')
  secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

  # Create an S3 client
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-southeast-2')  # Sydney region

  try:
      # List all files in the bucket
      contents = s3.list_objects(Bucket=bucket_name).get('Contents')
      if not contents:
          return "Bucket is already empty or does not exist"

      print(f"Deleting {len(contents)} objects from {bucket_name}")

      # Prepare a list of objects to delete
      objects_to_delete = [{'Key': file['Key']} for file in contents]

      # Delete the files
      delete_response = s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
      deleted_files = delete_response.get('Deleted', [])

      print(f"Successfully deleted {len(deleted_files)} files")

      if 'Errors' in delete_response:
          print(f"Errors encountered: {delete_response['Errors']}")

      return f"All files deleted from bucket {bucket_name}"
  except Exception as e:
      print(f"Error deleting files from bucket {bucket_name}: {e}")
      return f"Error deleting files from bucket {bucket_name}: {e}"



@app.route('/update-database', methods=['GET'])
def update_database():
    directory = os.getcwd() + '/assets'
    latest_file = find_latest_file(directory)
    if not latest_file:
        return jsonify({'error': 'No files found in assets directory'}), 404

    # Unzip and process the latest TSV file
    tsv_data = unzip_and_process_tsv(os.path.join(directory, latest_file))
    if tsv_data.empty:
        return jsonify({'error': 'Failed to process TSV file'}), 500

    # Update the database with the TSV data
    update_status = update_db_with_tsv_data(tsv_data)
    return jsonify({'message': update_status})

def find_latest_file(directory):
    files = [f for f in os.listdir(directory) if f.endswith('.gz') and 'segments' in f]
    if not files:
        return None

    def extract_date_and_version(filename):
        # Split the filename on slashes, underscores, and periods to isolate date and version components
        parts = filename.replace('.gz', '').split('/')
        segments_part = None
        for part in parts:
            if 'segments' in part:
                segments_part = part
                break
        if segments_part is None:
            return datetime.min, 0  # Return minimum date and version if 'segments' not in path
        
        filename_parts = segments_part.split('_')
        date_part = None
        version_part = None
        for part in filename_parts:
            if part.isdigit():
                if len(part) == 8:  # Date pattern yyyymmdd
                    date_part = part
                elif len(part) == 3:  # Version pattern xxx
                    version_part = part

        if date_part and version_part:
            return datetime.strptime(date_part, '%Y%m%d'), int(version_part)
        else:
            return datetime.min, 0  # Return minimum date and version if not found

    # Sort files based on extracted date and version number, handling files without proper naming
    files.sort(key=extract_date_and_version, reverse=True)

    return files[0]



def unzip_and_process_tsv(gz_path):
    try:
        with gzip.open(gz_path, 'rt') as f:
            return pd.read_csv(f, sep='\t')
    except Exception as e:
        print(f"Error processing TSV file: {e}")
        return pd.DataFrame()

def update_db_with_tsv_data(tsv_data):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('PGDATABASE'),
            user=os.getenv('PGUSER'),
            password=os.getenv('PGPASSWORD'),
            host=os.getenv('PGHOST'),
            port=os.getenv('PGPORT')
        )
        cur = conn.cursor()

        # Your SQL logic here to insert/update data
        # Example: cur.execute("INSERT INTO your_table (columns...) VALUES (%s, %s, ...)", (values...))

        conn.commit()
        cur.close()
        conn.close()
        return "Database updated successfully"
    except Exception as e:
        print(f"Database update failed: {e}")
        return "Database update failed"


@app.route('/upload-latest-to-s3', methods=['GET'])
def upload_latest_to_s3():
    directory = os.getcwd() + '/assets'
    latest_file = find_latest_file(directory)
    print(f"Latest Segment File: {latest_file}")
    if not latest_file:
        return jsonify({'error': 'No files found in assets directory'}), 404

    # Configure S3 access
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('TRITON_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('TRITON_SECRET_ACCESS_KEY')
    )

    # Extract the date from the filename
    date_part = latest_file.split('.')[1]  # Assuming the filename format is always as described

    bucket_name = 'triton-dmp-integrations'
    # Update the s3_key_prefix to include the new folder structure and the extracted date
    s3_key_prefix = f'prod/near/41793/segments/{date_part}/'

    # Full path to the latest file
    file_path = os.path.join(directory, latest_file)

    # Extract the original filename by removing the prefix up to "full."
    original_filename = latest_file[latest_file.index('full.'):]

    # S3 key for the file uses the original filename to preserve its naming convention
    s3_key = s3_key_prefix + secure_filename(original_filename)

    # Upload the file to S3
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        return jsonify({'message': f'File {original_filename} uploaded successfully to s3://{bucket_name}/{s3_key}'})
    except Exception as e:
        return jsonify({'error': f'Failed to upload {original_filename} to S3: {str(e)}'}), 500


@app.route('/local-upload-to-folder', methods=['POST'])
def local_upload_to_folder():
    """
    Route to upload a local file from the 'uploads' directory to a specific S3 folder.
    Validates the filename based on the folder_name and constructs the S3 path accordingly.
    """
    filename = request.form.get('filename')
    folder_name = request.form.get('folder_name')

    if not filename or not folder_name:
        return jsonify({'error': 'Both filename and folder_name are required'}), 400

    date_str = None
    
    # --- Validation and Date Extraction ---
    if folder_name == 'segments':
        # Pattern: {"full" or "inc"}.{YYYYMMDD}.{NNN}.ip.tsv.gz
        match = re.match(r'^(full|inc)\.(\d{8})\.\d{3}\.ip\.tsv\.gz$', filename)
        if not match:
            return jsonify({
                'error': 'Invalid filename for "segments" folder.',
                'message': 'Filename must follow the format: {"full" or "inc"}.{YYYYMMDD}.{NNN}.ip.tsv.gz'
            }), 400
        date_str = match.group(2) # The second capture group is the 8-digit date
    elif folder_name == 'taxonomy':
        # Pattern: {YYYYMMDD}.{NNN}.taxonomy.tsv.gz
        match = re.match(r'^(\d{8})\.\d{3}\.taxonomy\.tsv\.gz$', filename)
        if not match:
            return jsonify({
                'error': 'Invalid filename for "taxonomy" folder.',
                'message': 'Filename must follow the format: {YYYYMMDD}.{NNN}.taxonomy.tsv.gz'
            }), 400
        date_str = match.group(1) # The first capture group is the 8-digit date
    else:
        return jsonify({'error': f'Invalid folder_name: "{folder_name}". Must be "segments" or "taxonomy".'}), 400

    # --- File Existence Check ---
    uploads_dir = os.path.join(os.getcwd(), 'uploads')
    local_file_path = os.path.join(uploads_dir, secure_filename(filename))

    if not os.path.isfile(local_file_path):
        return jsonify({'error': f'File {secure_filename(filename)} not found in uploads folder'}), 404

    # --- S3 Upload Logic ---
    triton_access_key = os.getenv('TRITON_ACCESS_KEY')
    triton_secret_key = os.getenv('TRITON_SECRET_ACCESS_KEY')
    s3_bucket = 'triton-dmp-integrations'

    # Construct the correct S3 key
    s3_key = f'prod/near/41793/{folder_name}/{date_str}/{secure_filename(filename)}'

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=triton_access_key,
            aws_secret_access_key=triton_secret_key,
            region_name='ap-southeast-2'
        )
        print(f"Uploading {local_file_path} to s3://{s3_bucket}/{s3_key}")
        s3.upload_file(local_file_path, s3_bucket, s3_key)
        
        # Provide positive and accurate feedback
        return jsonify({
            'message': 'File uploaded successfully.',
            'details': {
                'filename': filename,
                's3_path': f's3://{s3_bucket}/{s3_key}'
            }
        })
    except Exception as e:
        return jsonify({'error': f'Failed to upload file to S3: {str(e)}'}), 500





@app.route('/manual-upload-to-triton', methods=['GET'])
def manual_upload_to_triton():
    near_bucket_name = 'arn-triton-prod'
    triton_bucket_name = 'triton-dmp-integrations'

    # Get the file name from request arguments
    file_name = request.args.get('file-name')
    if not file_name:
        return jsonify({'error': 'File name is required'}), 400

    # Extract the date from the file name assuming the format "near/YYYYMMDD/segments/full.YYYYMMDD.001.ip.tsv.gz"
    # Here we split the filename and take the second part which should be YYYYMMDD
    try:
        date_str = file_name.split('/')[1]  # Extracts the 'YYYYMMDD' part
        date = datetime.strptime(date_str, '%Y%m%d')  # Converts string to datetime object
    except (IndexError, ValueError) as e:
        return jsonify({'error': f'Invalid file name format: {str(e)}'}), 400

    # Download file from Near bucket
    near_s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='ap-southeast-2'
    )

    # Ensure the "temp" directory exists for temporary storage
    temp_dir = 'temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Temporary file path
    temp_file_path = os.path.join(temp_dir, secure_filename(file_name.split('/')[-1]))  # Using the actual file name part only

    try:
        near_s3.download_file(near_bucket_name, file_name, temp_file_path)
    except Exception as e:
        return jsonify({'error': f'Failed to download {file_name} from Near bucket: {str(e)}'}), 500

    # Upload file to Triton bucket
    triton_s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('TRITON_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('TRITON_SECRET_ACCESS_KEY')
    )

    # Construct the target S3 key using the extracted date
    s3_key_prefix = f'prod/near/41793/segments/{date_str}/'
    triton_s3_key = f'{s3_key_prefix}full.{date_str}.001.ip.tsv.gz'
    print(f"Uploaded to: {triton_s3_key}")

    try:
        triton_s3.upload_file(temp_file_path, triton_bucket_name, triton_s3_key)
    except Exception as e:
        return jsonify({'error': f'Failed to upload to Triton: {str(e)}'}), 500
    finally:
        # Clean up: remove the temporary file
        os.remove(temp_file_path)

    return jsonify({'message': f'File uploaded successfully to s3://{triton_bucket_name}/{triton_s3_key}'})





def convert_size(size_bytes):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:3.1f} {x}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"

@app.route('/list-triton-files', methods=['GET'])
def list_triton_files():
    bucket_name = 'prod'  
    prefix = ''  # Adjusted to match your CLI example

    # Use NEAR credentials from environment variables
    near_access_key_id = os.getenv('TRITON_ACCESS_KEY')
    near_secret_access_key = os.getenv('TRITON_SECRET_ACCESS_KEY')

    # Ensure the NEAR credentials are set
    if not near_access_key_id or not near_secret_access_key:
        return jsonify({'error': 'NEAR credentials are not set in environment variables'}), 500

    s3 = boto3.client(
        's3',
        aws_access_key_id=near_access_key_id,
        aws_secret_access_key=near_secret_access_key
    )

    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        files = []
        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    files.append({
                        'Key': item['Key'],
                        'Size': convert_size(item['Size']),
                        'LastModified': item['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                    })

        # Summary information (optional)
        total_files = len(files)
        total_size = sum([item['Size'] for item in files])

        return jsonify({'files': files, 'total_files': total_files, 'total_size': convert_size(total_size)})

    except Exception as e:
        return jsonify({'error': f'Failed to list files: {str(e)}'}), 500

    
@app.route('/validate-taxonomy', methods=['GET'])
def validate_taxonomy():
    # Define the file paths
    taxonomy_file_path = 'D:/Projects/S3-Bucket-Near-Triton/assets/near_20240207_taxonomy_20240207.001.taxonomy.tsv'
    segment_file_path = 'D:/Projects/S3-Bucket-Near-Triton/assets/near_20240207_segments_full.20240207.001.ip.tsv'

    # Read the taxonomy file
    taxonomy_df = pd.read_csv(taxonomy_file_path, sep='\t', header=None, names=['Segment ID', 'Segment Name', 'Price', 'Status'])

    # Read only the first 100 lines of the segment file for testing
    segment_df = pd.read_csv(segment_file_path, sep='\t', nrows=10000)

    # Initialize an empty list to store the results
    results = []

    # Iterate through each row in the taxonomy DataFrame
    for _, row in taxonomy_df.iterrows():
        # Count occurrences of the Segment ID in the segment-ids column of the segment DataFrame
        count = segment_df['segment-ids'].apply(lambda x: str(row['Segment ID']) in str(x).split(',')).sum()

        # Convert count from int64 to int for JSON serialization and append the result to the list
        results.append({'Segment Name': row['Segment Name'], 'Count': int(count)})

    # Return the results as a JSON response
    return jsonify(results)

@app.route('/get-triton-files', methods=['GET'])
def get_triton_files():
    """
    List files in the Triton S3 bucket using TRITON credentials.
    Returns a JSON list of files with key, size, and last modified date.
    """
    bucket_name = 'triton-dmp-integrations'
    prefix = ''  # Optionally allow filtering by prefix in the future

    triton_access_key = os.getenv('TRITON_ACCESS_KEY')
    triton_secret_key = os.getenv('TRITON_SECRET_ACCESS_KEY')

    if not triton_access_key or not triton_secret_key:
        return jsonify({'error': 'Triton credentials are not set in environment variables'}), 500

    s3 = boto3.client(
        's3',
        aws_access_key_id=triton_access_key,
        aws_secret_access_key=triton_secret_key,
        region_name='ap-southeast-2'
    )

    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        files = []
        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    files.append({
                        'Key': item['Key'],
                        'Size': convert_size(item['Size']),
                        'LastModified': item['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                    })

        total_files = len(files)
        total_size = sum([item['Size'] if isinstance(item['Size'], int) else 0 for item in page['Contents']]) if files else 0

        return jsonify({'files': files, 'total_files': total_files})
    except Exception as e:
        return jsonify({'error': f'Failed to list files in Triton bucket: {str(e)}'}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
