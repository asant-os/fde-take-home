from urllib.parse import urlparse
import pyarrow.fs as pafs

def open_uri(uri: str) -> tuple[pafs.FileSystem, str]:
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if scheme == "file":
        return pafs.LocalFileSystem(), parsed.path
    
    elif scheme == "gs":
        file_sys = pafs.GcsFileSystem()
        path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"
        return file_sys, path
    
    elif scheme == "s3":
        file_sys = pafs.S3FileSystem()
        path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"
        return file_sys, path
    
    else:
        raise ValueError(f"Unsupported URI scheme: '{scheme}'")