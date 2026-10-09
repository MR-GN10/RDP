#!/
# Leaving Rishu 
import os
import sys
import json
import time
import random
import hashlib
import hmac
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# COLORS
# ============================================================
try:
    from colorama import init, Fore, Style
    init(autoreset=True)
    G, Y, R, B, C, M, W = (Fore.GREEN, Fore.YELLOW, Fore.RED, Fore.BLUE,
                           Fore.CYAN, Fore.MAGENTA, Fore.WHITE)
    BOLD, DIM, RST = Style.BRIGHT, Style.DIM, Style.RESET_ALL
except ImportError:
    G = Y = R = B = C = M = W = BOLD = DIM = RST = ""


def log_info(m):    print(f"{C}[i]{RST} {m}", flush=True)
def log_ok(m):      print(f"{G}[+]{RST} {m}", flush=True)
def log_warn(m):    print(f"{Y}[!]{RST} {m}", flush=True)
def log_err(m):     print(f"{R}[-]{RST} {m}", flush=True)
def log_debug(m):   print(f"{DIM}    {m}{RST}", flush=True)


# ============================================================
# CONSTANTS
# ============================================================
AES_KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
AES_IV  = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])

CLIENT_SECRET = "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3"
APP_ID        = 100067

OAUTH_REGISTER_URL    = "https://100067.connect.garena.com/api/v2/oauth/guest:register"
OAUTH_TOKEN_URL       = "https://100067.connect.garena.com/oauth/guest/token/grant"
MAJOR_REGISTER_URL    = "https://loginbp.ppmainecoonghj.com/MajorRegister"
NEWBIE_URL            = "https://loginbp.ppmainecoonghj.com/ChooseNewbieChoice"
MAJOR_LOGIN_URL       = "https://loginbp.ppmainecoonghj.com/MajorLogin"
GENERATE_NICKNAME_URL = "https://loginbp.ppmainecoonghj.com/GenerateNickname"
GETLOGIN_HOST_IND     = "client.ind.freefiremobile.com"

REGION = "IND"                              # <-- forced IND
OUTPUT_FILE = "accounts.json"
BATCH_SIZE  = 100                           # <-- activate every 100

# XOR key for MajorRegister field 14
XOR_KEY = [0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,
           0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30]

# Field 22 device signature
FIELD_22_HEX = (
    "474752450101010062020000a78910bd098e3ff2e4345d59a31db114ea088f37e32e65"
    "212ff96621793d9eb78720d0bf2ac95176569765247ada5eb01376b3b9931794a4946"
    "d76ef8890779f3f2129317e6e1cb2fdcf7b06247cea343b8d4f167eff85a2e1dfe99"
    "b4583a7e1a155dbe7f7f85cff3b223eb77222ec1228f3ee1ef6ce7f8ca24b00a554"
    "e497328812f8df74c82d519ae3e3ceab436eb145e8a517089a7cef6a4efb22214b2"
    "a4b19989b74807584afe5e52825e7ac60e19a596a9bf02d961de6a0ed2515ec6023"
    "fdb7684d9464b97b21c527ce61b6bee4ef30d20a1fa33a996952d44d44e44f2f86c"
    "768aaf2a7808ad60f91048dca0207961ba7c2555c48341b30190debc775edb2cee24"
    "4cf51fca3760ce4388be2db45e80b813b5beb9c784007b7762d7a1e428affc8a3a4"
    "cd76cbaef648a274297fde33233dccd3f272cb77f39a1affe0365a24954111f768f72"
    "0e77535af024bea2b2726c3bac992374755c3deacf09235e6865d456e651680d115a"
    "751a797225eaacdf9a513cf104526e1a32e5296e111a33ae581a63850837921df848"
    "9adfd41ea895b7cf3f5b2e45d538a6e4f032f590ccbb7daf5fa9c50adadee0799661"
    "4c3f957bda349e6c484fdf55970d1943ad7955a76671298b6d98b636b69ebde6bc94"
    "dbd93ac3393a6ae230130445b2d744189167854a5617be2393e7d8fbb5719a1b4754"
    "1ba466167e3e05a6c244f1301ee2035acf94dffc8adbde747d5cd85e35ead3acc372"
    "a59c4220e54bf63f9d80485f3de2518495c1d0f78c911d2da595911fd2a1989cf17"
    "cf3ded5f6c92dea64d675c555c11df92d6c517ca5d0d61a8962f43f76ec7e87596c"
    "8325ebf9ab0f8e6d2eca33c511ceac6980906ebd68c478665591dcecf788ec6ef34c"
    "fbe3fcc279c6147c5bf91cd43cdc9704236"
)


# ============================================================
# FILE WRITE LOCK
# ============================================================
_file_lock = threading.Lock()


def save_account(account):
    """Thread-safe append to accounts.json."""
    with _file_lock:
        try:
            with open(OUTPUT_FILE, "r") as f:
                data = json.load(f)
        except Exception:
            data = []
        data.append(account)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


def update_account(uid, updates):
    """Thread-safe update of one account by uid."""
    with _file_lock:
        try:
            with open(OUTPUT_FILE, "r") as f:
                data = json.load(f)
        except Exception:
            data = []
        for entry in data:
            if str(entry.get("uid")) == str(uid):
                entry.update(updates)
                break
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


# ============================================================
# PROTOBUF WRITER
# ============================================================
def write_varint(value: int) -> bytes:
    out = []
    while value > 127:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def write_varint_field(f, v):
    return write_varint((f << 3) | 0) + write_varint(v)


def write_string_field(f, v):
    b = v.encode("utf-8") if isinstance(v, str) else v
    return write_varint((f << 3) | 2) + write_varint(len(b)) + b


def write_bytes_field(f, v):
    return write_varint((f << 3) | 2) + write_varint(len(v)) + v


def assemble_proto(fields):
    data = bytearray()
    for k, v in fields.items():
        if isinstance(v, int):
            data.extend(write_varint_field(k, v))
        elif isinstance(v, str):
            data.extend(write_string_field(k, v))
        elif isinstance(v, bytes):
            data.extend(write_bytes_field(k, v))
    return bytes(data)


# ============================================================
# PROTOBUF READER
# ============================================================
def read_varint(data, offset):
    r, s = 0, 0
    while True:
        b = data[offset]; offset += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80):
            break
        s += 7
    return r, offset


def parse_proto(data):
    out = {}
    off = 0
    while off < len(data):
        try:
            tag, off = read_varint(data, off)
        except Exception:
            break
        f = tag >> 3
        w = tag & 0x7
        if f == 0:
            break
        try:
            if w == 0:
                v, off = read_varint(data, off)
                out[f] = v
            elif w == 2:
                ln, off = read_varint(data, off)
                val = data[off:off + ln]
                off += ln
                try:
                    out[f] = val.decode("utf-8")
                except Exception:
                    out[f] = val.hex()
            else:
                break
        except Exception:
            break
    return out


# ============================================================
# AES
# ============================================================
def aes_encrypt(plain):
    return AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(pad(plain, AES.block_size))


def aes_decrypt(ciphertext):
    if not ciphertext or len(ciphertext) % 16 != 0:
        return ciphertext
    try:
        try:
            return unpad(AES.new(AES_KEY, AES.MODE_CBC, AES_IV).decrypt(ciphertext),
                         AES.block_size)
        except ValueError:
            return AES.new(AES_KEY, AES.MODE_CBC, AES_IV).decrypt(ciphertext)
    except Exception:
        return ciphertext


# ============================================================
# XOR open_id
# ============================================================
def xor_open_id(open_id: str) -> bytes:
    b = open_id.encode("utf-8")
    return bytes(x ^ XOR_KEY[i % len(XOR_KEY)] for i, x in enumerate(b))


# ============================================================
# RETRY WRAPPER
# ============================================================
def retry(func, retries=3, delay=1.0, backoff=2.0):
    """Call func() with retries + exponential backoff. Returns result or None."""
    last = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(delay * (backoff ** (attempt - 1)))
    log_debug(f"retry exhausted: {last}")
    return None


# ============================================================
# STEP 1 — Register guest
# ============================================================
def register_guest(session, password):
    payload = {"app_id": APP_ID, "client_type": 2, "password": password, "source": 2}
    body = json.dumps(payload, separators=(",", ":"))
    sig = hmac.new(CLIENT_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    headers = {
        "User-Agent": "GarenaMSDK/4.0.42(KB2003 ;Android 13;en;HK;app 2.130.1 2019118332;)",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "Authorization": f"Signature {sig}",
    }
    r = session.post(OAUTH_REGISTER_URL, data=body, headers=headers, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    d = r.json()
    if d.get("code") != 0:
        raise RuntimeError(f"register failed: {d.get('message')}")
    return str(d["data"]["uid"])


# ============================================================
# STEP 2 — Token grant
# ============================================================
def get_token(session, uid, password):
    payload = {
        "uid": uid, "password": password, "response_type": "token",
        "client_type": "2", "client_secret": CLIENT_SECRET, "client_id": "100067",
    }
    headers = {
        "User-Agent": "GarenaMSDK/4.0.30",
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "Connection": "Keep-Alive",
    }
    r = session.post(OAUTH_TOKEN_URL, data=payload, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    d = r.json()
    at = d.get("access_token")
    oi = d.get("open_id")
    if not at or not oi:
        raise RuntimeError("token missing fields")
    return at, oi


# ============================================================
# STEP 3 — GenerateNickname
# ============================================================
def generate_nickname(session, open_id):
    plain = assemble_proto({1: "en", 2: open_id})
    encrypted = aes_encrypt(plain)
    headers = {
        "Accept-Encoding": "gzip",
        "Authorization": "Bearer",
        "Connection": "Keep-Alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Expect": "100-continue",
        "Host": GENERATE_NICKNAME_URL.split("/")[2],
        "ReleaseVersion": "OB55",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "X-GA": "v1 1",
        "X-Unity-Version": "2018.4.12f1",
    }
    r = session.post(GENERATE_NICKNAME_URL, data=encrypted, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    name = r.text.strip()
    if not name:
        raise RuntimeError("empty nickname")
    return name


# ============================================================
# STEP 4 — MajorRegister
# ============================================================
def send_major_register(session, nickname, access_token, open_id):
    fields = {
        1:  nickname,
        2:  access_token,
        3:  open_id,
        5:  102000007,
        6:  4,
        7:  1,
        13: 1,
        14: xor_open_id(open_id),
        15: "TW",
        16: 2,
        20: "2.133.8",
        21: 1,
        22: bytes.fromhex(FIELD_22_HEX),
    }
    encrypted = aes_encrypt(assemble_proto(fields))
    headers = {
        "Host":             MAJOR_REGISTER_URL.split("/")[2],
        "User-Agent":       "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept":           "*/*",
        "Accept-Encoding":  "deflate, gzip",
        "X-GA-SV":          str(int(time.time())),
        "Authorization":    "Bearer",
        "X-GA":             "v1 1",
        "ReleaseVersion":   "OB55",
        "Content-Type":     "application/x-www-form-urlencoded",
        "X-Unity-Version":  "2018.4.12f1",
        "Content-Length":   str(len(encrypted)),
    }
    r = session.post(MAJOR_REGISTER_URL, data=encrypted, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    parsed = parse_proto(r.content)
    aid = parsed.get(3)
    if not aid:
        raise RuntimeError("no account_id")
    return int(aid)


# ============================================================
# STEP 5 — ChooseNewbieChoice
# ============================================================
def send_newbie(session, account_id, choice=3):
    plain = assemble_proto({1: int(account_id), 2: 1, 3: int(choice)})
    encrypted = aes_encrypt(plain)
    headers = {
        "Host": "loginbp.ppmainecoonghj.com",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*",
        "Accept-Encoding": "deflate, gzip",
        "X-GA-SV": str(int(time.time())),
        "Authorization": "Bearer",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB55",
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(encrypted)),
    }
    r = session.post(NEWBIE_URL, headers=headers, data=encrypted,
                     timeout=30, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return True


# ============================================================
# STEP 6 — MajorLogin
# ============================================================
def send_major_login(session, access_token, open_id, platform="4"):
    fields = {
        3: time.strftime("%Y-%m-%d %H:%M:%S"),
        4: "free fire", 5: 1, 7: "2.133.9",
        8: "Android OS 10 / API-29 (QP1A.190711.020/1617006012)",
        9: "Handheld", 10: "Vi India", 11: "WIFI",
        12: 1600, 13: 720, 14: "320",
        15: "ARM64 FP ASIMD AES | 2301 | 8", 16: 2799,
        17: "PowerVR Rogue GE8320",
        18: "OpenGL ES 3.2 build 1.11@5425693",
        19: "Google|9f7d6b8b-b10c-454a-852d-06332cd498eb",
        20: f"{random.randint(1,223)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        21: "en", 22: open_id, 23: str(platform),
        24: "Handheld", 25: "realme RMX2189", 26: "TW",
        29: access_token, 30: 1,
        41: "Vi India", 42: "WIFI",
        57: "1ac4b80ecf0478a44203bf8fac6120f5",
        60: 19799, 61: 2536, 62: 5056, 64: 2768,
        65: 19999, 66: 2536, 67: 19799,
        73: 1,
        74: "/data/app/com.dts.freefiremax-ShI7E0dK8p1IiZ785pvuVQ==/lib/arm64",
        76: 2,
        77: "38f4751a330688ab124c2c804cec90a5|/data/app/com.dts.freefiremax-ShI7E0dK8p1IiZ785pvuVQ==/base.apk",
        78: 2, 79: 2, 81: "64", 83: "2019118527",
        86: "OpenGLES3", 87: 3071, 88: 4, 92: 67920,
        93: "android_max",
        94: "KqsHT+UrR1HKqb6+1db+Ofei+NtZr2+hbiBo3yKDL8w+8E3S5qF2IgEEe1fFQFyHRzl4iyHjHp+QsfeLbjJ6+DidTiKxm0ak2uYYa6QR4nAUdlZR",
        95: 111107,
        96: '{"cur_rate":null,"support_etc2":true}',
        97: 1, 98: 1, 99: "4", 100: "4", 102: "",
        104: 83812, 105: 1,
        106: "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|a4332cb1c1a84e51dd77441e4856ed5a",
        107: "1.9393e7b8e53e8aeb",
    }
    encrypted = aes_encrypt(assemble_proto(fields))
    headers = {
        "Host": "loginbp.ppmainecoonghj.com",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*",
        "Accept-Encoding": "deflate, gzip",
        "X-GA-SV": str(int(time.time())),
        "Authorization": "Bearer",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB55",
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(encrypted)),
    }
    r = session.post(MAJOR_LOGIN_URL, data=encrypted, headers=headers,
                     timeout=30, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")

    raw = r.content
    for buf in (aes_decrypt(raw), raw[64:] if len(raw) > 64 else b"", raw):
        if not buf:
            continue
        try:
            parsed = parse_proto(buf)
        except Exception:
            continue
        jwt = parsed.get(8)
        if isinstance(jwt, bytes):
            try:
                jwt = jwt.decode("utf-8", "ignore")
            except Exception:
                jwt = ""
        if isinstance(jwt, str) and jwt.startswith("eyJ") and jwt.count(".") == 2:
            # session key/iv from 16-byte blobs
            sixteen = []
            for fnum in sorted(parsed.keys()):
                v = parsed[fnum]
                vals = v if isinstance(v, list) else [v]
                for val in vals:
                    if isinstance(val, (bytes, bytearray)) and len(val) == 16:
                        sixteen.append(bytes(val))
            k = sixteen[-2] if len(sixteen) >= 2 else AES_KEY
            i_ = sixteen[-1] if len(sixteen) >= 2 else AES_IV
            return jwt, k, i_
    raise RuntimeError("no JWT")


# ============================================================
# STEP 7 — GetLoginData (ACTIVATION)
# ============================================================
def send_getlogindata(session, jwt, open_id, platform="4"):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    fields = {
        3: now, 4: "free fire", 5: 1, 7: "2.133.9",
        8: "Android OS 15 / API-35 (V2UUIS35.39-21-7-5-1-5/319a58-a9271)",
        9: "Handheld", 10: "airtel", 11: "CarrierDataNetwork",
        12: 2400, 13: 1080, 14: "400",
        15: "ARM64 FP ASIMD AES | 4800 | 8", 16: 7461,
        17: "Adreno (TM) 710",
        18: "OpenGL ES 3.2 V@0615.98 (GIT@0c393b63cf, I94e2bd5684, 1746191168) (Date:05/02/25)",
        19: "Google|0ea5b865-94e6-42dd-9f40-f52a63378bc7",
        20: "27.59.76.15", 21: "en",
        22: open_id, 23: str(platform),
        24: "Handheld", 25: "motorola moto g96 5G", 26: "TW",
        29: jwt, 30: 1,
        41: "airtel", 42: "4G",
        57: "1ac4b80ecf0478a44203bf8fac6120f5",
        60: 111316, 61: 73226, 62: 690, 64: 73354,
        65: 111316, 66: 73354, 67: 111316,
        73: 3,
        74: "/data/app/~~xI7rymUmXZFbQjaBVyScAw==/com.dts.freefiremax-lX62T7VsUJ9zo5dV1_axug==/lib/arm64",
        76: 2,
        77: "38f4751a330688ab124c2c804cec90a5|/data/app/~~xI7rymUmXZFbQjaBVyScAw==/com.dts.freefiremax-lX62T7VsUJ9zo5dV1_axug==/base.apk",
        78: 2, 79: 2, 81: "64", 83: "2019118527", 85: 3,
        86: "OpenGLES3", 87: 4095, 88: 4,
        90: "New Delhi", 91: "DL", 92: 12573, 93: "android_max",
        94: "KqsHT0wswggWbev04P17TGvl/w+c875HviaAj4qL+YhymI7Psonj/aQgNTNLf+4nm1LzYE6DxxzpZH9FX0Kt119zYuGdivC/V7aWpj4/HNhVAwJy",
        95: 111107,
        96: '{"cur_rate":[60,90,120,144],"support_etc2":true}',
        97: 1, 99: "0", 100: "4", 102: "",
        104: 95926, 105: 1,
        106: "https://dl.cdn.freefiremobile.com/live/ABHotUpdates/|https://dl-core.cdn.freefiremobile.com/live/ABHotUpdates/|a4332cb1c1a84e51dd77441e4856ed5a",
        107: "1.7a99d677ab872769",
    }
    encrypted = aes_encrypt(assemble_proto(fields))
    url = f"https://{GETLOGIN_HOST_IND}/GetLoginData"
    headers = {
        "Host": GETLOGIN_HOST_IND,
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*",
        "Accept-Encoding": "deflate, gzip",
        "X-GA-SV": str(int(time.time())),
        "Authorization": f"Bearer {jwt}",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB55",
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(encrypted)),
    }
    r = session.post(url, headers=headers, data=encrypted,
                     timeout=30, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")

    raw = r.content
    for buf in (aes_decrypt(raw), raw[64:] if len(raw) > 64 else b"", raw):
        if not buf:
            continue
        try:
            p = parse_proto(buf)
        except Exception:
            continue
        online = p.get(14)
        if isinstance(online, bytes):
            try: online = online.decode("utf-8", "ignore")
            except Exception: online = ""
        if online and ":" in str(online):
            chat = p.get(32)
            if isinstance(chat, bytes):
                try: chat = chat.decode("utf-8", "ignore")
                except Exception: chat = ""
            return str(online), str(chat) if chat else None
    raise RuntimeError("no gateway")


# ============================================================
# GENERATE ONE ACCOUNT (steps 1-6)
# ============================================================
def generate_one(idx, total, retries):
    session = requests.Session()
    session.verify = False
    password = hashlib.sha256(os.urandom(32)).hexdigest().upper()

    # 1) register
    uid = retry(lambda: register_guest(session, password), retries=retries)
    if not uid:
        log_err(f"[{idx}] register failed"); return None

    # 2) token
    res = retry(lambda: get_token(session, uid, password), retries=retries)
    if not res:
        log_err(f"[{idx}] token failed for {uid}"); return None
    access_token, open_id = res

    # 3) nickname
    nickname = retry(lambda: generate_nickname(session, open_id), retries=retries)
    if not nickname:
        log_err(f"[{idx}] nickname failed for {uid}"); return None

    # 4) major register
    account_id = retry(lambda: send_major_register(session, nickname, access_token, open_id),
                       retries=retries)
    if not account_id:
        log_err(f"[{idx}] major register failed for {uid}"); return None

    # 5) newbie
    retry(lambda: send_newbie(session, account_id, 3), retries=retries)

    # 6) major login
    login = retry(lambda: send_major_login(session, access_token, open_id),
                  retries=retries)
    if not login:
        log_err(f"[{idx}] major login failed for {uid}"); return None
    jwt, session_key, session_iv = login

    account = {
        "uid":          uid,
        "password":     password,
        "nickname":     nickname,
        "open_id":      open_id,
        "access_token": access_token,
        "account_id":   account_id,
        "region":       REGION,
        "jwt":          jwt,
        "session_key":  session_key.hex(),
        "session_iv":   session_iv.hex(),
        "activated":    False,
        "created_at":   time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    log_ok(f"[{idx}] generated {uid}  nick={nickname}")
    return account


# ============================================================
# ACTIVATE ONE ACCOUNT (step 7)
# ============================================================
def activate_one(account, retries):
    session = requests.Session()
    session.verify = False
    res = retry(lambda: send_getlogindata(session, account["jwt"], account["open_id"], "4"),
                retries=retries)
    if not res:
        log_err(f"activate failed {account['uid']}")
        return account

    online, chat = res
    account["online_ip_port"] = online
    account["chat_ip_port"]   = chat
    account["activated"]      = True
    account["activated_at"]   = time.strftime("%Y-%m-%d %H:%M:%S")

    update_account(account["uid"], {
        "online_ip_port": online,
        "chat_ip_port":   chat,
        "activated":      True,
        "activated_at":   account["activated_at"],
    })
    log_ok(f"activated {account['uid']}  online={online}  chat={chat}")
    return account


# ============================================================
# MAIN
# ============================================================
def main():
    print(f"{BOLD}{M}")
    print("╔══════════════════════════════════════════════╗")
    print("║   BULK ACCOUNT GENERATOR — IND ONLY          ║")
    print("║   Activation every 100 accounts              ║")
    print("╚══════════════════════════════════════════════╝")
    print(RST)

    # ── prompts ──
    try:
        total = int(input(f"{C}Total accounts to generate: {RST}").strip() or "100")
    except ValueError:
        total = 100
    try:
        threads = int(input(f"{C}Generation threads (1-20, default 5): {RST}").strip() or "5")
    except ValueError:
        threads = 5
    try:
        retries = int(input(f"{C}Retries per step (1-10, default 3): {RST}").strip() or "3")
    except ValueError:
        retries = 3
    try:
        act_threads = int(input(f"{C}Activation threads (1-20, default 10): {RST}").strip() or "10")
    except ValueError:
        act_threads = 10

    threads     = max(1, min(threads, 20))
    act_threads = max(1, min(act_threads, 20))
    retries     = max(1, min(retries, 10))

    print()
    log_info(f"Region          : {REGION}")
    log_info(f"Total           : {total}")
    log_info(f"Gen threads     : {threads}")
    log_info(f"Act threads     : {act_threads}")
    log_info(f"Retries         : {retries}")
    log_info(f"Batch size      : {BATCH_SIZE}")
    log_info(f"Output          : {OUTPUT_FILE}")
    print()

    # ── main loop ──
    generated_ok = 0
    generated_fail = 0
    batch = []              # accounts waiting for activation
    batch_num = 0
    t0 = time.time()

    def flush_batch():
        nonlocal batch, batch_num
        if not batch:
            return
        batch_num += 1
        log_info(f"━━━ ACTIVATING BATCH #{batch_num} ({len(batch)} accounts) ━━━")
        act_ok = 0
        act_fail = 0
        with ThreadPoolExecutor(max_workers=act_threads) as act_pool:
            futs = [act_pool.submit(activate_one, acc, retries) for acc in batch]
            for f in as_completed(futs):
                r = f.result()
                if r and r.get("activated"):
                    act_ok += 1
                else:
                    act_fail += 1
        log_info(f"Batch #{batch_num} done:  ✅ {act_ok}  ❌ {act_fail}")
        batch = []

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(generate_one, i + 1, total, retries)
                   for i in range(total)]
        for f in as_completed(futures):
            acc = f.result()
            if not acc:
                generated_fail += 1
                continue
            generated_ok += 1
            save_account(acc)
            batch.append(acc)

            # when batch fills → activate
            if len(batch) >= BATCH_SIZE:
                flush_batch()

        # activate the tail
        if batch:
            flush_batch()

    elapsed = time.time() - t0
    print()
    print(f"{BOLD}{C}══════════════════════════════════════════════{RST}")
    print(f"{BOLD}  DONE in {elapsed:.1f}s{RST}")
    print(f"{G}  ✅ Generated : {generated_ok}{RST}")
    print(f"{R}  ❌ Failed    : {generated_fail}{RST}")
    print(f"{C}  📄 Saved to  : {OUTPUT_FILE}{RST}")
    print(f"{BOLD}{C}══════════════════════════════════════════════{RST}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Y}Interrupted{RST}")