#!/usr/bin/env python3

from pathlib import Path
from sys import argv

from sofia_utils.io import ( load_json_file,
                             load_json_string,
                             write_to_json_file )
from sofia_utils.printing import print_ind

from .do_bucket_io import *


def check_environment_variables() -> None :
    """
    Print current bucket-related environment variables and client config \\
    """
    
    print("=== Checking Environment Variables ===")
    
    # Check each variable
    variables = { "BUCKET_REGION"    : BUCKET_REGION,
                  "BUCKET_KEY"       : BUCKET_KEY,
                  "BUCKET_KEY_SECRET": BUCKET_KEY_SECRET,
                  "BUCKET_NAME"      : BUCKET_NAME }
    
    print("Environment variables as constants:")
    for var, value in variables.items() :
        if value :
            if not "key" in var.lower() :
                print_ind( f"{var}: {value}", 1)
            else :
                # Mask sensitive values
                masked_value = "*" * len(value)
                print_ind( f"{var}: {masked_value}", 1)
        else :
            print_ind( f"{var}: ❌ NOT SET", 1)
    
    print()
    
    # Test the boto3 client creation
    print("=== Testing Boto3 Client Creation ===")
    print("Boto3 client arguments:")
    for key, value in boto3_client_args.items() :
        if not "key" in key.lower() :
            print_ind( f"{key}: {value}", 1)
        else :
            masked_value = "*" * len(value)
            print_ind( f"{key}: {masked_value}", 1)
    
    print("Creating boto3 client...")
    try :
        b3 = boto3_client(**boto3_client_args)
        print("✅ Boto3 client created successfully!")
        del b3
    except Exception as e :
        print(f"❌ Boto3 client creation failed: {e}")
    
    return

def test_do_bucket_io( filepath_json : str, filepath_media : str) -> None :
    """
    Upload/download a JSON file and media asset for manual verification \\
    Args:
        filepath_json  : Path to a JSON file to upload
        filepath_media : Path to a media file to upload/download
    """
    
    # Extract filenames without paths
    fn_json  = Path(filepath_json).expanduser().name
    fn_media = Path(filepath_media).expanduser().name
    
    # Test parameters
    user_id = "user_test"
    case_id = "1"
    key_json  = Path(user_id) / case_id / "data" / fn_json
    key_media = Path(user_id) / case_id / "media" / fn_media
    
    print("=== Testing DigitalOcean Spaces Functions ===")
    
    # Stage 1: Upload JSON
    print("\nStage 1: Uploading JSON...")
    try :
        if not Path(filepath_json).exists() :
            print(f"❌ JSON file not found: {filepath_json}")
            return
        
        print_ind(f"key: {key_json}")
        json_data = load_json_file(filepath_json)
        b3_put_json( key_json, json_data)
        print(f"✅ JSON uploaded successfully to key: {key_json}")
    
    except Exception as e :
        print(f"❌ JSON upload failed: {e}")
        return
    
    # Stage 2: Upload Media
    print("\nStage 2: Uploading media...")
    try :
        if not Path(filepath_media).exists() :
            print(f"❌ Media file not found: {filepath_media}")
            return
        with open( filepath_media, 'rb') as f :
            media_content = f.read()
        
        print_ind(f"key: {key_media}")
        result = b3_put_media( key_media, media_content, "image/jpeg")
        print( f"✅ Media uploaded successfully to bucket: " \
             + f"{result['bucket']}, key: {result['key']}" )
    
    except Exception as e :
        print(f"❌ Media upload failed: {e}")
        return
    
    # Stage 3: Download JSON
    print("\nStage 3: Downloading JSON...")
    try :
        output_json_path = f"downloaded_{fn_json}"
        downloaded_json  = load_json_string( b3_get_file(key_json))
        print(f"✅ JSON downloaded successfully")
        
        write_to_json_file( output_json_path, downloaded_json)
        print_ind( f"Saved to: {output_json_path}", 1)
    
    except Exception as e :
        print(f"❌ JSON download failed: {e}")
        return
    
    # Stage 4: Download Media
    print("\nStage 4: Downloading media...")
    try :
        output_media_path = f"downloaded_{fn_media}"
        downloaded_media  = b3_get_file(key_media)
        print(f"✅ Media downloaded successfully")
        print_ind( f"Downloaded size: {len(downloaded_media)} bytes", 1)
        
        with open( output_media_path, 'wb') as f :
            f.write(downloaded_media)
        
        print_ind( f"Saved to: {output_media_path}", 1)
    
    except Exception as e :
        print(f"❌ Media download failed: {e}")
        return
    
    print("\n=== All tests completed successfully! ===")
    
    return

if __name__ == "__main__" :
    
    if not len(argv) == 3 :
        fname = str(Path(__file__).name)
        print(f"Usage: python {fname} <JSON file> <Media file>")
    else :
        check_environment_variables()
        test_do_bucket_io( argv[1], argv[2])
