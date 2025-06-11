import os
import acb
import traceback
from io import BytesIO
from pathlib import Path
from PyCriCodecs import HCA
from typing import Union, Optional
from pydub import AudioSegment


def extract_acb(file_path: Union[Path, str], binary_data: Optional[bytes] = None) -> bool:
    """Extract ACB file, decrypt HCA file to wav, convert wav to mp3 and save, then remove the ACB file."""
    file_path = str(file_path)  # Convert file_path type to string
    try:
        _save_dir = os.path.dirname(file_path)  # Save to source file folder
        os.makedirs(_save_dir, exist_ok=True)  # Make sure save_dir exists

        if binary_data:
            acb.extract_acb(BytesIO(binary_data), _save_dir)  # Extract HCA from bytes
        else:
            acb.extract_acb(file_path, _save_dir)  # Extract HCA from ACB file

        for hca_file in os.listdir(_save_dir):
            if hca_file.endswith(".hca"):
                # Prepare save path for MP3 file
                save_file_path = os.path.join(_save_dir, os.path.splitext(hca_file)[0] + ".mp3")

                # Decode HCA to WAV format (using HCA codec)
                hca_decoder = HCA(os.path.join(_save_dir, hca_file), key=88888888)
                wav_data = hca_decoder.decode()

                # Convert WAV (from decoded HCA) to temporary file using BytesIO
                wav_temp_path = os.path.join(_save_dir, os.path.splitext(hca_file)[0] + ".wav")
                with open(wav_temp_path, "wb") as wav_file:
                    wav_file.write(wav_data)

                # Convert WAV to MP3 using pydub
                audio = AudioSegment.from_wav(wav_temp_path)
                audio.export(save_file_path, format="mp3")  # Export as MP3

                # Remove temporary WAV and HCA files
                os.remove(wav_temp_path)
                os.remove(os.path.join(_save_dir, hca_file))  # Remove HCA file

        if os.path.exists(file_path):
            os.remove(file_path)  # Remove ACB file

        return True
    except Exception as e:
        traceback.print_exc()
        return False
