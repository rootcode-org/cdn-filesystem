# Copyright is waived. No warranty is provided. Unrestricted use and modification is permitted.

import os
import sys
import gzip
import json
import base64
import hashlib
from io import BytesIO
from hashlib import md5

PURPOSE = """\
Store/Retrieve incremental file system snapshots to/from cloud storage

cdnfs.py push                                Upload a snapshot to a storage service bucket
cdnfs.py list <snapshot_id>                  List files in a snapshot
cdnfs.py get  <snapshot_id> <download_path>  Download a snapshot

where,
   <snapshot_id>    Snapshot identifier (hash of root manifest file)
   <download_path>  Path to folder for downloaded snapshot
"""

CONFIGURATION = {

    #####
    # Mandatory configuration
    ####

    # Specify local path to folder to snapshot
    "local_path": "",

    # Specify a bucket name here to enable upload to S3
    "s3_bucket_name": "",

    # Specify a project name and bucket parameters here to enable upload to GCS
    "gcp_project_name": "",
    "gcs_bucket_name": "",
    "gcs_bucket_uniform": False,    # set True if bucket has uniform bucket-level access control, otherwise False

    #####
    # Optional configuration
    ####

    # File exclusions from local path. Currently this only excludes exact file name matches
    "local_exclusions": [".DS_Store"],

    # Specify number of bits of file hash to use for cloud file names and to store in manifest files
    "manifest_hash_bits": 80,

    # Specify if uploaded files should have a public ACL set
    # !!Obviously take great care with this. Do not make files public unless you are really sure you need to!!
    "set_public_acl": False,

    # Files with these extensions will be gzip compressed during upload and have their Content-Encoding header set
    "gzip_types": [".txt", ".htm", ".html", ".css", ".csv", ".js", ".json"],

    # The Cache-Control header will be set to this value for all uploaded files; default is the maximum age allowed
    "cache_control": "public,max-age=31536000",

    # The Content-Type header will be set according to the file extension
    # Any file type not represented here will be set to "application/octet-stream"
    "content_types": {
        ".txt":  "text/plain; charset=utf-8",
        ".htm":  "text/html; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".css":  "text/css; charset=utf-8",
        ".csv":  "text/csv; charset=utf-8",
        ".js":   "application/javascript; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".xml":  "application/xml; charset=utf-",
        ".bin":  "application/octet-stream",
        ".pdf":  "application/pdf",
        ".ogx":  "application/ogg",
        ".zip":  "application/zip",
        ".bmp":  "image/bmp",
        ".ico":  "image/x-icon",
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png":  "image/png",
        ".tiff": "image/tiff",
        ".oga":  "audio/ogg",
        ".mp4a": "audio/mp4",
        ".wav":  "audio/x-wav",
        ".ogv":  "video/ogg",
        ".mp4":  "video/mp4"
    }
}

# Import APIs for enabled cloud services
if "s3_bucket_name" in CONFIGURATION and CONFIGURATION["s3_bucket_name"]:
    try:
        import boto3
    except ImportError:
        sys.exit("Requires Boto3 module; try 'pip install boto3'")

if "gcp_project_name" in CONFIGURATION and CONFIGURATION["gcp_project_name"]:
    try:
        from google.cloud import storage
    except ImportError:
        sys.exit("Requires Google Cloud Storage module; try 'pip install google-cloud-storage'")


# Base class for storage services
class Storage:

    def __init__(self):
        self.name = None
        self.make_public = False
        self.listing=[]

    def list_storage(self):
        raise NotImplementedError()        # provided by derived class

    def file_exists(self, hash):
        return hash in self.listing

    def put_file(self, hash, data, content_type, cache_control, compress):
        raise NotImplementedError()        # provided by derived class

    def get_file(self, hash):
        raise NotImplementedError()        # provided by derived class

    def upload_snapshot(self, base_path, relative_path, local_exclusions, manifest_hash_digits, cache_control, content_types, gzip_types):
        manifest = {}
        local_path = os.path.join(base_path, relative_path)
        for item in os.listdir(local_path):
            item_path = os.path.join(local_path, item)
            if os.path.isdir(item_path):
                # Recurse on subfolders
                relative_path = item_path[len(base_path)+1:]
                folder_hash = self.upload_snapshot(base_path, relative_path, local_exclusions, manifest_hash_digits, cache_control, content_types, gzip_types)
                if folder_hash:
                    manifest[item] = (folder_hash, 0)       # a size of 0 denotes a subfolder

            elif os.path.isfile(item_path):
                if item not in local_exclusions:            # ignore excluded files
                    file_size = os.path.getsize(item_path)
                    if file_size > 0:                       # ignore zero-length files
                        # Upload file to storage
                        with open(item_path, "rb") as f:
                            file_data = f.read()
                            file_hash = hashlib.sha256(file_data).hexdigest()[0:manifest_hash_digits]
                            if not self.file_exists(file_hash):     # skip if this file is already in storage
                                print("Uploading file     {0} ({1} bytes) as {2}".format(item_path[len(base_path)+1:], file_size, file_hash))
                                root, ext = os.path.splitext(item_path)
                                content_type = content_types[ext] if ext in content_types else "application/octet-stream"
                                compress = ext in gzip_types
                                self.put_file(file_hash, file_data, content_type, cache_control, compress)
                                self.listing.append(file_hash)
                        manifest[item] = (file_hash, file_size)
            else:
                print("WARNING: {0} is a symbolic link and is being ignored".format(item))

        if manifest is not None:
            # Sort manifest entries; convert to dictionary and dump to json
            manifest = sorted(manifest.items(), key=lambda x: (int(x[1][1]), x[0]))
            manifest_json = json.dumps({k: v for k, v in manifest}).encode("latin_1")

            # Upload manifest json
            manifest_hash = hashlib.sha256(manifest_json).hexdigest()[0:manifest_hash_digits]
            if not service.file_exists(manifest_hash):
                name = local_path[len(base_path)+1:]
                name = name if name else "[root]"
                print("Uploading manifest {0} ({1} bytes) as {2}".format(name, len(manifest_json), manifest_hash))
                service.put_file(manifest_hash, manifest_json, "application/json; charset=utf-8", cache_control, True)
            return manifest_hash

    def list_snapshot(self, snapshot_identifier, parent_path):
        manifest = json.loads(service.get_file(snapshot_identifier))
        for key, (hash, size) in manifest.items():
            item_path = parent_path + "/" + key if parent_path else key
            if int(size) == 0:
                self.list_snapshot(hash, item_path)
            else:
                print("{0} {1:>10}  {2}".format(hash, size, item_path))

    def download_snapshot(self, snapshot_identifier, parent_path, download_path):
        manifest = json.loads(service.get_file(snapshot_identifier))
        for key, (hash, size) in manifest.items():
            item_path = parent_path + "/" + key if parent_path else key
            if int(size) == 0:
                self.download_snapshot(hash, item_path, download_path)
            else:
                print("Downloading {0}".format(item_path))
                data = service.get_file(hash)
                file_path = os.path.join(download_path, item_path)
                folder_path = os.path.dirname(file_path)
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
                with open(file_path, "wb") as f:
                    f.write(data)


class S3Storage(Storage):

    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html for Boto3 API documentation
    def __init__(self, bucket_name, make_public):
        Storage.__init__(self)
        self.name = "S3"
        self.make_public = make_public
        self.bucket = boto3.resource('s3').Bucket(bucket_name)
        if not self.bucket.creation_date:
            sys.exit("Alert: Specify a bucket that exists")

    def list_storage(self):
        self.listing = [x.key for x in self.bucket.objects.all()]

    def put_file(self, hash, data, content_type, cache_control, compress):
        acl = "public-read" if self.make_public else "private"
        if compress:
            data = gzip.compress(data, compresslevel=9)
        md5_hash_b64 = base64.b64encode(md5(data).digest()).decode("latin_1")
        if compress:
            self.bucket.put_object(Key=hash, Body=data, ACL=acl, ContentType=content_type, ContentMD5=md5_hash_b64, CacheControl=cache_control, ContentEncoding="gzip")
        else:
            self.bucket.put_object(Key=hash, Body=data, ACL=acl, ContentType=content_type, ContentMD5=md5_hash_b64, CacheControl=cache_control)

    def get_file(self, hash):
        obj = self.bucket.Object(hash)
        with BytesIO() as f:
            obj.download_fileobj(f)
            data = f.getvalue()
        if obj.content_encoding == "gzip":
            data = gzip.decompress(data)
        return data


class GCSStorage(Storage):

    # see https://googleapis.dev/python/storage/latest/index.html for Google Cloud Storage API documentation
    def __init__(self, project_name, bucket_name, bucket_uniform, make_public):
        Storage.__init__(self)
        if make_public and bucket_uniform:
            sys.exit("Alert: Fix configuration conflict; Can not set public ACL on bucket with uniform access control")
        self.name = "GCS"
        self.make_public = make_public
        self.client = storage.Client(project=project_name)
        self.bucket = self.client.lookup_bucket(bucket_name)
        self.is_uniform = bucket_uniform

    def list_storage(self):
        self.listing = [x.name for x in self.bucket.list_blobs()]

    def put_file(self, hash, data, content_type, cache_control, compress):
        blob = self.bucket.blob(hash)
        if compress:
            data = gzip.compress(data, compresslevel=9)
            blob.content_encoding = "gzip"
        blob.cache_control = cache_control
        blob.upload_from_string(bytes(data), content_type)
        if self.make_public and not self.is_uniform:
            blob.make_public()

    def get_file(self, hash):
        blob = self.bucket.blob(hash)
        data = blob.download_as_string()
        if blob.content_encoding == "gzip":
            data = gzip.decompress(data)
        return data


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit(PURPOSE)

    # Pick a service to use based on configuration
    if "s3_bucket_name" in CONFIGURATION and CONFIGURATION["s3_bucket_name"]:
        service = S3Storage(CONFIGURATION["s3_bucket_name"], CONFIGURATION["set_public_acl"])
    elif "gcp_project_name" in CONFIGURATION and CONFIGURATION["gcp_project_name"]:
        service = GCSStorage(CONFIGURATION["gcp_project_name"], CONFIGURATION["gcs_bucket_name"], CONFIGURATION["gcs_bucket_uniform"], CONFIGURATION["set_public_acl"])
    else:
        sys.exit("Alert: Configure a service in CONFIGURATION")

    command = sys.argv[1].lower()
    if command == "push":
        local_path = CONFIGURATION["local_path"]
        if not local_path:
            sys.exit("Alert: Specify a local path to folder to snapshot")
        local_path = os.path.expanduser(local_path)
        if not os.path.exists(local_path):
            sys.exit("Alert: Specify a local path that exists")
        local_exclusions = CONFIGURATION["local_exclusions"]
        manifest_hash_bits = CONFIGURATION["manifest_hash_bits"]
        if manifest_hash_bits > 256:
            sys.exit("Alert: Set number of hash bits to 256 or less")
        manifest_hash_digits = int((manifest_hash_bits + 3) / 4)        # number of hex digits for file identifiers
        cache_control = CONFIGURATION["cache_control"]
        content_types = CONFIGURATION["content_types"]
        gzip_types = CONFIGURATION["gzip_types"]

        # List all files that currently exist in the storage service
        print("Listing files on {0} service".format(service.name))
        service.list_storage()

        # Create a snapshot and upload new files and manifests
        print("Uploading files to {0} service".format(service.name))
        snapshot_identifier = service.upload_snapshot(local_path, "", local_exclusions, manifest_hash_digits, cache_control, content_types, gzip_types)
        print("Snapshot identifier is {0}".format(snapshot_identifier))

    elif command == "list":
        if len(sys.argv) < 3:
            sys.exit("Alert: Specify a snapshot identifier")
        service.list_snapshot(sys.argv[2], "")

    elif command == "get":
        if len(sys.argv) < 4:
            sys.exit("Alert: Provide all parameters")
        service.download_snapshot(sys.argv[2], "", sys.argv[3])
