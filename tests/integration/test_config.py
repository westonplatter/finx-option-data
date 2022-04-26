import sys
sys.path.append("..")
from finx_option_data.configs import Config
import os


def test_bucket_name():    
    file_dir = os.path.dirname(os.path.realpath('__file__'))
    stage = os.getenv("STAGE", "prod").lower()
    file_name = f".env.{stage}"
    full_path = os.path.join(file_dir, f'./{file_name}')
    
    config = Config(full_path)
    assert config.bucket_name == "qdl-prod-lake"



    
    