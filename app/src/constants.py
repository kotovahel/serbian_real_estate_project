from pathlib import Path

BASE_URL = 'https://katastar.rgz.gov.rs/RegistarCenaNepokretnosti/'

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/json',
    'Origin': 'https://katastar.rgz.gov.rs',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://katastar.rgz.gov.rs/RegistarCenaNepokretnosti/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'trailers',
}

INPUT_FOLDER = Path("../data/")
OUTPUT_FOLDER = Path("../data/output")

REPLACE_DICT = {'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'ђ': 'đ', 'е': 'e', 'ж': 'ž', 'з': 'z', 'и': 'i',
                'ј': 'j', 'к': 'k', 'л': 'l', 'љ': 'lj', 'м': 'm', 'н': 'n', 'њ': 'nj', 'о': 'o', 'п': 'p', 'р': 'r',
                'с': 's', 'т': 't', 'ћ': 'ć', 'ч': 'č', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'c', 'џ': 'dž', 'ш': 'š', ' ': ' '}

