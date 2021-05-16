import os
import subprocess
import random
from subprocess import CalledProcessError
from pymediainfo import MediaInfo


class SceneTime:

    thumb_dir = ''

    @staticmethod
    def generate_thumbnails(video_path, media_info, out_dir, hashval,
                            out_interval=300, out_format='jpeg', vlc_bin='cvlc'):
        hash_dir = os.path.join(out_dir, hashval[:2])
        if not os.path.isdir(hash_dir):
            os.mkdir(hash_dir)
        duration = SceneTime.media_length(media_info)
        times = [(i * out_interval) + random.randrange(out_interval)
                 for i in range(duration // out_interval)]
        for i, t in enumerate(times):
            out_prefix = '{0}_{1:2d}_'.format(hashval, i)
            cmd = SceneTime.generate_thumb_cmd(video_path, t, hash_dir, out_prefix,
                                               out_format=out_format, vlc_bin=vlc_bin)
            print("Running cmd: " + ' '.join(cmd))
            try:
                ret = subprocess.check_call(cmd)
            except CalledProcessError:
                with open(os.path.join(out_dir, 'errors.txt'), 'a') as f:
                    f.write(f'Error with {video_path} at time {t}\n')
                break

    @staticmethod
    def generate_thumb_cmd(video_path, timestamp, out_dir, out_prefix, out_format='jpeg', vlc_bin='cvlc'):
        cmd = [vlc_bin,
               video_path,
               f'--start-time={timestamp}',
               f'--stop-time={timestamp + 1}',
               '--aout=none',
               '--vout=dummy',
               '--video-filter=scene',
               '--scene-ratio=120',
               f'--scene-format={out_format}',
               f'--scene-path={out_dir}',
               f'--scene-prefix={out_prefix}',
               'vlc://quit'
              ]
        return cmd

    @staticmethod
    def get_media_info(video_path):
        '''
        :param video_path: path to video
        :return: mediainfo object
        '''
        return MediaInfo.parse(video_path)

    @staticmethod
    def media_length(media_info):
        '''
        :param media_info:
        :return runtime in seconds (int):
        '''
        return media_info.to_data()['tracks'][0]['duration'] // 1000

    @staticmethod
    def file_ext(file_path):
        '''
        :param file_path:
        :return: file extension (str)
        '''
        return os.path.splitext(os.path.basename(file_path))[1][1:].lower()