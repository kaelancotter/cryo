import hashlib
import xxhash
from Crypto.Hash import MD4


class Hashmatic:

    @staticmethod
    def sha1(data):
        return hashlib.sha1(data).hexdigest()

    @staticmethod
    def sha256(data):
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def sha512(data):
        return hashlib.sha512(data).hexdigest()

    @staticmethod
    def md5(data):
        return hashlib.md5(data).hexdigest()

    @staticmethod
    def xh128(data):
        return xxhash.xxh128_hexdigest(data)

    # based on https://github.com/jtojnar/ed2k/blob/master/ed2k.py
    @staticmethod
    def ed2k(data):
        def md4(x):
            h = MD4.new()
            h.update(x)
            return h
        ed2k_block = 9500 * 1024
        ed2k_hash = b''
        for block_idx in range(len(data) // ed2k_block):
            start = block_idx * ed2k_block
            stop = (block_idx + 1) * ed2k_block
            cur_block = data[start: stop]
            ed2k_hash += md4(cur_block).digest()
        if len(data) % ed2k_block == 0:
            ed2k_hash += md4('').digest()
        return md4(ed2k_hash).hexdigest()
