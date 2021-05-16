
from multiprocessing import Pool
from functools import partial
from scene_time import SceneTime
from hash_matic import Hashmatic
from ice_maker import IceMaker
import os
import json






class Trial:

    @staticmethod
    def par_test(start_dir, num_proc=4):
        with Pool(processes=num_proc) as p:
            p.map(print, Indexer.iter_dir_files(start_dir))

    @staticmethod
    def unique_file_types(start_dir):
        found_types = set()
        for f in Indexer.iter_dir_files(start_dir):
            ext = os.path.splitext(os.path.basename(f))[1][1:]
            if ext not in found_types:
                found_types.add(ext)
        return found_types


class Indexer:

    @staticmethod
    def load_env(env_path):
        with open(env_path, 'r') as f:
            for l in f.readlines():
                k, v = l.rstrip().split('=')
                os.environ[k] = v

    @staticmethod
    def iter_dir_files(start_dir):
        for dirpath, dirnames, filenames in os.walk(start_dir):
            for d in dirnames:
                Indexer.iter_dir_files(os.path.join(dirpath, d))
            for f in filenames:
                yield os.path.join(dirpath, f)

    @staticmethod
    def par_fn_dir(start_dir, proc_fn=None, num_proc=4):
        if not proc_fn:
            raise ValueError
        with Pool(processes=num_proc) as p:
            p.map(proc_fn, Indexer.iter_dir_files(start_dir))

    @staticmethod
    def safe_file_proc(file_path, proc_fn=None):
        try:
            proc_fn(file_path)
        except Exception:
            with IceMaker('') as i:
                i.error(file_path)

    @staticmethod
    def validate_file(file_path, table=''):
        with open(file_path, 'rb') as f:
            file_data = f.read()
        sha512 = Hashmatic.sha512(file_data)
        del file_data
        with IceMaker('anime_tape1') as ice:
            hash_found = ice.exists_hash(sha512)
        if not hash_found:
            with IceMaker('') as ice:
                ice.hash_fail(file_path)

    @staticmethod
    def process_file(file_path, table=''):
        # early exit if already processed
        with IceMaker(table) as ice:
            already_processed = ice.exists(file_path)
        if already_processed:
            print(f"Already Processed: {file_path}")
            return

        #read file data
        with open(file_path, 'rb') as f:
            file_data = f.read()
        hash_id = Hashmatic.sha512(file_data)

        #generate metadata
        stats = {
            'sha512': hash_id,
            'fn': os.path.basename(file_path),
            'dir': os.path.dirname(file_path),
            'sha1': Hashmatic.sha1(file_data),
            'sha256': Hashmatic.sha256(file_data),
            'md5': Hashmatic.md5(file_data),
            'ed2k': Hashmatic.ed2k(file_data),
            'xxh128': Hashmatic.xh128(file_data),
            'metadata': '"null"',
            'fsize': len(file_data)
        }
        del file_data

        # generate thumbnails
        ext = SceneTime.file_ext(file_path)
        if ext in ['ogm', 'mp4', 'mkv', 'avi']:
            media_info = SceneTime.get_media_info(file_path)
            stats['metadata'] = json.dumps(media_info.to_data())
            try:
                SceneTime.generate_thumbnails(file_path, media_info, SceneTime.thumb_dir, hash_id)
            except KeyError:
                print(f"No duration in {file_path} metadata")

        # push metadata to db
        with IceMaker(table) as ice:
            ice.freeze(**stats)



if __name__ == '__main__':
    Indexer.load_env('./local_env')
    #proc_dir = '/tank/anime/tape/anime_tape1/'
    #proc_dir = '/home/arjuna/mnt/anime/tape/check/'
    #SceneTime.thumb_dir = proc_dir[:-1] + '_thumb/'
    #assert os.path.isdir(proc_dir)
    #assert os.path.isdir(SceneTime.thumb_dir)
    #par_process_dir(proc_dir)
    #validate_dir = '/tank/anime/tape/anime_tape1_read/'
    validate_dir = '/tank/anime/tape/test_read/'
    Indexer.par_validate_dir(validate_dir)
    #print(unique_file_types('/tank/anime/tape/test/'))