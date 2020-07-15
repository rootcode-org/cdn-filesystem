# CDN File System

Take incremental file system snapshots and store them in S3 or GCS storage.

Ideal for games and applications that want to stream versioned assets.

## How to use
1. Configure the local folder and cloud service parameters in cdnfs.py
2. Ensure you have service credentials available locally per the needs of Boto3 and/or Google Cloud Storage API 
2. Run cdnfs.py to upload/list/download snapshots

Also note the "public_acl" setting. Set this to 'True' to enable a public ACL to be set on your uploaded files.  Note
if you are using a Google Cloud Storage bucket with uniform access-control then this setting cannot be applied.

After an upload take note of the snapshot identifier. This is the file name of the manifest for the root folder and
 must be provided to your application in order to locate the files belonging to the snapshot.

## What does it do?
The local folder is scanned and all files are uploaded to cloud storage named after the SHA256 hash of the file
content (the hash length is configurable, by default 80-bits of the hash are used for the name). In addition a
manifest is generated for each subfolder that maps file names to the associated hash name used in cloud storage.

- The snapshot identifier represents exactly the contents of the local folder at the time the snapshot is taken, and that
 version is available forever.
- Each time you create a snapshot only files that are new or that have changed are uploaded, along with only the
 manifest files that have changed.
- Only unique files are uploaded. If you have the same file in multiple locations in your local folders, only a single
 copy of that file is uploaded.

## Parsing manifest files
You will need to parse the manifest files in your application. Don't worry, it's easy. Each manifest is simply a JSON
 file that represents the contents of a single folder, for example;

    {
     "images": ["2d203094375715af6122", 0],
     "models": ["44136fa355b3678a1146", 0],
     "config.json": ["ee173537981a7f76f6c6", 157],
     "balancing.csv": ["75ea31874e50833c40b3", 5430]
    }


Items in the JSON can be either a file or a subfolder. The item key is the name of the file or subfolder, and the value
is a list with 2 entries; the hash name of the file, and the size in bytes.  Where the size is 0 bytes the entry is a
subfolder and the file is the manifest for that subfolder. In the example above 'images' and 'models' are subfolders,
while 'config.json' and 'balancing.csv' are files.

To locate a file you therefore start with the root manifest file, and load the manifest for each path element until
you get to the file entry you want.

Manifests are gzip encoded and stored with a content type of 'application/json; charset=utf-8'

enjoy!
 
frankie@rootcode.org
